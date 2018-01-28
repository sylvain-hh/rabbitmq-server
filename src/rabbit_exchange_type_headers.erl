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

% Now a binding has an order which is part of the new mnesia table used during route/2
% Will be "per binding" a bit later.
-define(DEFAULT_BINDING_ORDER, 200).


info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{description, <<"AMQP headers exchange, as per the AMQP specification">>}].

serialise_events() -> false.


route(X, #delivery{message = #basic_message{content = Content}}) ->
    Headers = case (Content#content.properties)#'P_basic'.headers of
                  undefined -> [];
                  H         -> rabbit_misc:sort_field_table(H)
              end,
    CurrentOrderedBindings = case ets:lookup(rabbit_headers_bindings, X) of
        [] -> [];
        [#headers_bindings{bindings = E}] -> E
    end,
    get_routes (Headers, CurrentOrderedBindings, []).

get_routes (_Headers, [], DestsResult) -> DestsResult;
get_routes (Headers, [ {_,_, BindingType, [MainDest], Args, []} | R ], Res) ->
    case BindingType of
        all ->
            case headers_match_all (Args, Headers) of
                true -> get_routes (Headers, R, [ MainDest | Res]);
                _ -> get_routes (Headers, R, Res)
            end;
        any ->
            case headers_match_any (Args, Headers) of
                true -> get_routes (Headers, R, [ MainDest | Res]);
                _ -> get_routes (Headers, R, Res)
            end
    end.


%%
%% Requires message headers to be sorted (bindings are via add_binding)
%% There is no more type checking; match operators have a {K,Operator,P} pattern
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


%% [0] spec is vague on whether it can be omitted but in practice it's
%% useful to allow people to do this
parse_x_match({longstr, <<"all">>}) -> all;
parse_x_match({longstr, <<"any">>}) -> any;
parse_x_match(_)                    -> all. %% legacy; we didn't validate


% get_match_operators : returns the "compiled form" to be stored in mnesia of binding args related to match operators; will be improved later.
% the explicit "ex" operator means "must exist", "eq" is "must be equal"
get_match_operators([], Result) -> Result;
%% It's not properly specified, but a "no value" in a
%% pattern field is supposed to mean simple presence of
%% the corresponding data field. I've interpreted that to
%% mean a type of "void" for the pattern field.
get_match_operators([ {K, void, _V} | N ], Res) ->
    get_match_operators (N, [ {K, ex, nil} | Res]);
% skip all x-* args..
get_match_operators([ {<<"x-", _/binary>>, _, _} | N ], Res) ->
    get_match_operators (N, Res);
% the default rule is value of key K must be equal to V
get_match_operators([ {K, _, V} | N ], Res) ->
    get_match_operators (N, [ {K, eq, V} | Res]).


add_binding(transaction, X, BindingToAdd = #binding{destination = MainDest, args = BindingArgs}) ->
% A binding have now an Id; part of the mnesia key table too
    BindingId = crypto:hash(md5, term_to_binary(BindingToAdd)),
    BindingOrder = ?DEFAULT_BINDING_ORDER,
    BindingType = parse_x_match(rabbit_misc:table_lookup(BindingArgs, <<"x-match">>)),
    MatchOperators = get_match_operators (BindingArgs, []),
    CurrentOrderedBindings = case mnesia:read (rabbit_headers_bindings, X, write) of
        [] -> [];
        [#headers_bindings{bindings = E}] -> E
    end,
    NewBinding = {BindingOrder, BindingId, BindingType, [MainDest], rabbit_misc:sort_field_table(MatchOperators), []},
    NewBindings = lists:keysort(1, [ NewBinding | CurrentOrderedBindings]),
    NewRecord = #headers_bindings{exchange = X, bindings = NewBindings},
    ok = mnesia:write (rabbit_headers_bindings, NewRecord, write);
add_binding(_Tx, _X, _B) ->
    ok.


remove_bindings(transaction, X, Bs) ->
    CurrentOrderedBindings = case mnesia:read (rabbit_headers_bindings, X, write) of
        [] -> [];
        [#headers_bindings{bindings = E}] -> E
    end,
    BindingIdHashesToDelete = [ crypto:hash (md5, term_to_binary(B)) || B <- Bs],
    NewOrderedBindings = [ Bind || Bind=[_,BId,_,_,_,_] <- CurrentOrderedBindings, lists:member(BId, BindingIdHashesToDelete) == false],
    NewRecord = #headers_bindings{exchange = X, bindings = NewOrderedBindings},
    ok = mnesia:write (rabbit_headers_bindings, NewRecord, write);
remove_bindings(_Tx, _X, _Bs) ->
    ok.


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


validate(_X) -> ok.
create(_Tx, _X) -> ok.
delete(_Tx, _X, _Bs) -> ok.
policy_changed(_X1, _X2) -> ok.
assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).
