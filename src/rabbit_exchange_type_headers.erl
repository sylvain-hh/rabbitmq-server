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

route(X,
      #delivery{message = #basic_message{content = Content}}) ->
    Headers = case (Content#content.properties)#'P_basic'.headers of
                  undefined -> [];
                  H         -> rabbit_misc:sort_field_table(H)
              end,
    BindingsIDs = ets:lookup(rabbit_headers_bindings_keys, X),
    get_routes (X, Headers, BindingsIDs, []).
 

get_routes (_X, _Headers, [], DestsResult) -> DestsResult;
get_routes (X, Headers, [ #headers_bindings_keys{binding_id=BindingId} | R ], Res) ->
    case ets:lookup(rabbit_headers_bindings, {X,BindingId}) of
        %% It may happen that a binding is deleted in the meantime (?)
        [] -> get_routes (X, Headers, R, Res);
        %% Binding type is all
        [#headers_bindings{destinations=Dest, binding_type=all, compiled_args=Args}] ->
            case headers_match_all (Args, Headers) of
                true -> get_routes (X, Headers, R, [Dest | Res]);
                _ -> get_routes (X, Headers, R, Res)
            end;
        %% Binding type is any
        [#headers_bindings{destinations=Dest, binding_type=any, compiled_args=Args}] ->
            case headers_match_any (Args, Headers) of
                true -> get_routes (X, Headers, R, [Dest | Res]);
                _ -> get_routes (X, Headers, R, Res)
            end
    end.


%%
%% Requires headers to be sorted as binding's args 
%%

%% Binding type 'all' checks

% No more binding rule to check; return true
headers_match_all([], _) -> true;
% No more message header but still binding's rule to check; return false
headers_match_all(_, []) -> false;
% Current header key not in binding; go next header with current binding's rule
headers_match_all(BCur = [{BK, _, _} | _], [{HK, _, _} | DNext])
    when BK > HK -> headers_match_all(BCur, DNext);
% Current binding key does not exist in message; return false
headers_match_all([{BK, _, _} | _], [{HK, _, _} | _])
    when BK < HK -> false;
%
% From here, BK == HK
%
% Binding rule value is equal to message header value; ok go next
headers_match_all([{_, eq, PV} | BNext], [{_, _, DV} | DNext])
    when PV == DV -> headers_match_all(BNext, DNext);
% Value do not match while operator is equality; return false
headers_match_all([{_, eq, _} | _], _) -> false;
% Operator is "must exist" and BK == HK; ok go next
headers_match_all([{_, ex, _} | BNext], [ _ | DNext]) ->
    headers_match_all(BNext, DNext).


%% Binding type 'any' checks

% No more binding rule to check; return false
headers_match_any([], _) -> false;
% No more header; return false
headers_match_any(_, []) -> false;
% Current header key not in binding; go next header with current binding's rule
headers_match_any(BCur = [{BK, _, _} | _], [{DK, _, _} | DNext])
    when BK > DK -> headers_match_any(BCur, DNext);
% Current binding key does not exist in message; go next binding
headers_match_any([{BK, _, _} | BNext], DCur = [{DK, _, _} | _])
    when BK < DK -> headers_match_any(BNext, DCur);
%
% From here, BK == HK
%
headers_match_any([{_, eq, BV} | _], [{_, _, DV} | _]) when DV == BV -> true;
headers_match_any([{_, ex, _} | _], _) -> true;
% No match yet; go next
headers_match_any([_ | BNext], DCur) ->
    headers_match_any(BNext, DCur).


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


%% Flatten one level bindings args (because of new array type usage)
flatten_binding_args(Args) ->
    flatten_binding_args(Args, []).
flatten_binding_args([], Result) -> Result;
flatten_binding_args ([ {K, array, Vs} | R ], Result) ->
    Res = [ { K, T, V } || {T, V} <- Vs ],
    flatten_binding_args (R, lists:append ([ Res , Result ]));
flatten_binding_args ([ {K, T, V} | R ], Result) ->
    flatten_binding_args (R, [ {K, T, V} | Result ]).


% get_match_operators : returns the "compiled form" to be stored in mnesia of binding args related to match operators
get_match_operators([], Result) -> Result;
% Reported comment (commit HASH)
%% It's not properly specified, but a "no value" in a
%% pattern field is supposed to mean simple presence of
%% the corresponding data field. I've interpreted that to
%% mean a type of "void" for the pattern field.
get_match_operators([ {K, void, _V} | N ], Res) ->
    get_match_operators (N, [ {K, ex, nil} | Res]);
% So, let's go for a properly identified new operator !
get_match_operators([ {<<"x-?ex">>, longstr, V} | N ], Res) ->
    get_match_operators (N, [ {V, ex, nil} | Res]);
% skip other x-* args..
get_match_operators([ {<<"x-", _/binary>>, _, _} | N ], Res) ->
    get_match_operators (N, Res);
% for all other cases, the default is value of key K must be equal to V
get_match_operators([ {K, _, V} | N ], Res) ->
    get_match_operators (N, [ {K, eq, V} | Res]).


add_binding(transaction, #exchange{name = #resource{virtual_host = VHost}} = X, BindingToAdd = #binding{destination = MainDest, args = Args}) ->
% Binding can have args whith array/table types, so let's flatten them first
    FlattenedArgs = flatten_binding_args(Args),
% Order is part of the mnesia key table, so that during route process, bindings will be read in order
    DefaultOrder = 200,
% A binding have now an Id; part of the mnesia key table too
    BindingId = crypto:hash(md5, term_to_binary(BindingToAdd)),
    BindingType = parse_x_match(rabbit_misc:table_lookup(FlattenedArgs, <<"x-match">>)),
    MatchOperators = get_match_operators (FlattenedArgs, []),
% Store the new exchange's bindingId
    NewBindingRecord = #headers_bindings_keys{exchange = X, binding_id = {DefaultOrder,BindingId}},
    ok = mnesia:write (rabbit_headers_bindings_keys, NewBindingRecord, write),
% Store the new binding details
    NewBindingDetails = #headers_bindings{exch_bind = {X, {DefaultOrder,BindingId}}, binding_type = BindingType, destinations = MainDest, compiled_args = rabbit_misc:sort_field_table(MatchOperators)},
    ok = mnesia:write (rabbit_headers_bindings, NewBindingDetails, write),
    %% Because ordered_bag does not exist, we need to reorder bindings
    %%  so that we don't need to sort them again in route/2
    OrderedBindings = lists:sort (mnesia:read (rabbit_headers_bindings_keys, X)),
    lists:foreach (fun(R) -> mnesia:delete_object (rabbit_headers_bindings_keys, R, write) end, OrderedBindings),
    lists:foreach (fun(R) -> ok = mnesia:write (rabbit_headers_bindings_keys, R, write) end, OrderedBindings);
add_binding(_Tx, _X, _B) -> ok.


validate(_X) -> ok.
create(_Tx, _X) -> ok.
delete(_Tx, _X, _Bs) -> ok.
policy_changed(_X1, _X2) -> ok.
remove_bindings(_Tx, _X, _Bs) -> ok.
assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).
