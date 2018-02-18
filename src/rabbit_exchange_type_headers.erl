%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type_headers).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/3, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([info/1, info/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type headers"},
                    {mfa,         {rabbit_registry, register,
                                   [exchange, <<"headers">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{description, <<"AMQP headers exchange, as per the AMQP specification">>}].

serialise_events() -> false.

route(#exchange{name = Name},
      #delivery{message = #basic_message{content = Content}}) ->
    Headers = case (Content#content.properties)#'P_basic'.headers of
                  undefined -> [];
                  H         -> rabbit_misc:sort_field_table(H)
              end,
    CurrentOrderedBindings = case mnesia:dirty_read(rabbit_headers_bindings, Name) of
        [] -> [];
        [#headers_bindings{bindings = E}] -> E
    end,
    get_routes (Headers, CurrentOrderedBindings, []).

get_routes (_, [], Dests) -> Dests;
get_routes (Headers, [ {_, BindingType, Dest, Args} | T ], Dests) ->
    case headers_match(Args, Headers, true, false, BindingType) of
        true -> get_routes (Headers, T, [ Dest | Dests]);
        _    -> get_routes (Headers, T, Dests)
    end.

validate_binding(_X, #binding{args = Args}) ->
    case rabbit_misc:table_lookup(Args, <<"x-match">>) of
        {longstr, <<"all">>} -> ok;
        {longstr, <<"any">>} -> ok;
        {longstr, Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field value ~p; "
                                  "expected all or any", [Other]}};
        {Type,    Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field type ~p (value ~p); "
                                  "expected longstr", [Type, Other]}};
        undefined            -> ok %% [0]
    end.
%% [0] spec is vague on whether it can be omitted but in practice it's
%% useful to allow people to do this

parse_x_match({longstr, <<"all">>}) -> all;
parse_x_match({longstr, <<"any">>}) -> any;
parse_x_match(_)                    -> all. %% legacy; we didn't validate

%% Horrendous matching algorithm. Depends for its merge-like
%% (linear-time) behaviour on the lists:keysort
%% (rabbit_misc:sort_field_table) that route/1 and
%% rabbit_binding:{add,remove}/2 do.
%%
%%                 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%% In other words: REQUIRES BOTH PATTERN AND DATA TO BE SORTED ASCENDING BY KEY.
%%                 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%%

% A bit less horrendous algorithm :)
headers_match(_, _, false, _, all) -> false;
headers_match(_, _, _, true, any) -> true;

% No more bindings, return current state
headers_match([], _Data, AllMatch, _AnyMatch, all) -> AllMatch;
headers_match([], _Data, _AllMatch, AnyMatch, any) -> AnyMatch;

% Delete bindings starting with x-
headers_match([{<<"x-", _/binary>>, _PT, _PV} | PRest], Data,
              AllMatch, AnyMatch, MatchKind) ->
    headers_match(PRest, Data, AllMatch, AnyMatch, MatchKind);

% No more data, but still bindings, false with all
headers_match(_Pattern, [], _AllMatch, AnyMatch, MatchKind) ->
    headers_match([], [], false, AnyMatch, MatchKind);

% Data key header not in binding, go next data
headers_match(Pattern = [{PK, _PT, _PV} | _], [{DK, _DT, _DV} | DRest],
              AllMatch, AnyMatch, MatchKind) when PK > DK ->
    headers_match(Pattern, DRest, AllMatch, AnyMatch, MatchKind);

% Binding key header not in data, false with all, go next binding
headers_match([{PK, _PT, _PV} | PRest], Data = [{DK, _DT, _DV} | _],
              _AllMatch, AnyMatch, MatchKind) when PK < DK ->
    headers_match(PRest, Data, false, AnyMatch, MatchKind);

%% It's not properly specified, but a "no value" in a
%% pattern field is supposed to mean simple presence of
%% the corresponding data field. I've interpreted that to
%% mean a type of "void" for the pattern field.
headers_match([{PK, void, _PV} | PRest], [{DK, _DT, _DV} | DRest],
              AllMatch, _AnyMatch, MatchKind) when PK == DK ->
    headers_match(PRest, DRest, AllMatch, true, MatchKind);

% Complete match, true with any, go next
headers_match([{PK, _PT, PV} | PRest], [{DK, _DT, DV} | DRest],
              AllMatch, _AnyMatch, MatchKind) when PK == DK andalso PV == DV ->
    headers_match(PRest, DRest, AllMatch, true, MatchKind);

% Value does not match, false with all, go next
headers_match([{PK, _PT, _PV} | PRest], [{DK, _DT, _DV} | DRest],
              _AllMatch, AnyMatch, MatchKind) when PK == DK ->
    headers_match(PRest, DRest, false, AnyMatch, MatchKind).


validate(_X) -> ok.
create(_Tx, _X) -> ok.

delete(transaction, #exchange{name = XName}, _) ->
    ok = mnesia:delete (rabbit_headers_bindings, XName, write);
delete(_, _, _) -> ok.

policy_changed(_X1, _X2) -> ok.

add_binding(transaction, #exchange{name = XName}, BindingToAdd = #binding{destination = Dest, args = BindingArgs}) ->
% BindingId is used to track original binding definition so that it is used when deleting later
    BindingId = crypto:hash(md5, term_to_binary(BindingToAdd)),
% Let's doing that heavy lookup one time only
    BindingType = parse_x_match(rabbit_misc:table_lookup(BindingArgs, <<"x-match">>)),
    CurrentOrderedBindings = case mnesia:read(rabbit_headers_bindings, XName, write) of
        [] -> [];
        [#headers_bindings{bindings = E}] -> E
    end,
    NewBinding = {BindingId, BindingType, Dest, rabbit_misc:sort_field_table(BindingArgs)},
    NewBindings = [NewBinding | CurrentOrderedBindings],
    NewRecord = #headers_bindings{exchange_name = XName, bindings = NewBindings},
    ok = mnesia:write(rabbit_headers_bindings, NewRecord, write);
add_binding(_, _, _) ->
    ok.

remove_bindings(transaction, #exchange{name = XName}, BindingsToDelete) ->
    CurrentOrderedBindings = case mnesia:read(rabbit_headers_bindings, XName, write) of
        [] -> [];
        [#headers_bindings{bindings = E}] -> E
    end,
    BindingIdsToDelete = [crypto:hash(md5, term_to_binary(B)) || B <- BindingsToDelete],
    NewOrderedBindings = [Bind || Bind={BId,_,_,_} <- CurrentOrderedBindings, lists:member(BId, BindingIdsToDelete) == false],
    NewRecord = #headers_bindings{exchange_name = XName, bindings = NewOrderedBindings},
    ok = mnesia:write(rabbit_headers_bindings, NewRecord, write);
remove_bindings(_, _, _) ->
    ok.

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).
