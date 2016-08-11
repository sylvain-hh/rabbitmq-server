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

%%----------------------------------------------------------------------------

-define(DEFAULT_GOTO_ORDER, undefined).

%%----------------------------------------------------------------------------

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


% possible optim to do : if there is no bindings, there is no need to sort
route(X, #delivery{message = #basic_message{content = Content}}) ->
    Headers = case (Content#content.properties)#'P_basic'.headers of
        undefined -> [];
        H         -> rabbit_misc:sort_field_table(H)
    end,
    BindingsIDs = ets:lookup(rabbit_headers_bindings_keys, X),
%io:format ("~p ", [length(BindingsIDs)]),
    get_destinations (X, Headers, BindingsIDs, ?DEFAULT_GOTO_ORDER, []).



% Retreive destinations from bindings ids
get_destinations (_X, _Headers, [], _, Dests) -> Dests;
get_destinations (X, Headers, [ #headers_bindings_keys{binding_id={CurrentOrder,_}} | R ], GotoOrder, Dests) when is_number(GotoOrder) andalso CurrentOrder < GotoOrder ->
%%io:format("Skip : ~p~p~n", [CurrentOrder, GotoOrder]),
    get_destinations (X, Headers, R, GotoOrder, Dests);
get_destinations (X, Headers, [ #headers_bindings_keys{binding_id=BindingId} | R ], GotoOrder, Dests) ->
%%io:format("Go next : ~p~p~n", [CurrentOrder, GotoOrder]),
    case ets:lookup(rabbit_headers_bindings, {X,BindingId}) of
        %% It may happen that a binding is deleted in the meantime (?)
        [] -> get_destinations (X, Headers, R, GotoOrder, Dests);
        %% Binding type is all
        [#headers_bindings{destination=Dest, binding_type=all, last_nxkey=LNXK, stop_on_match=SOM, gotos={GOT,GOF}, dontroute=DontRoute, cargs=TransformedArgs}] ->
%%io:format("Cu GOT GOF : ~p~p~p~n", [CurrentOrder, GOT, GOF]),
	    case { DontRoute, SOM, lists:member(Dest, Dests) } of
		%% if destination is already matched, go next binding
		{ _, _, true } -> get_destinations (X, Headers, R, GotoOrder, Dests);
		%% bad use : do not route and stop anyway, ending with already matched bindings
		{ true, any, _ } -> [Dests];
		_ -> case { headers_match_all(TransformedArgs, Headers, LNXK), DontRoute, SOM } of
			 %% binding dont match and stop, ending with already matched bindings
			 { false, _, any } -> Dests;
			 { false, _, false } -> Dests;
			 %% binding dont match, go next binding
			 { false, _, _ } -> get_destinations (X, Headers, R, GOF, Dests);
			 %% binding match and stop but dont route, ending with already matched bindings
			 { _, true, any } -> Dests;
			 { _, true, true } -> Dests;
			 %% binding match but dont route, go next binding
			 { _, true, _ } -> get_destinations (X, Headers, R, GOT, Dests);
			 %% binding match and stop but route, ending with new dest
			 { _, false, any } -> [Dest | Dests];
		         { _, false, true } -> [Dest | Dests];
			 %% binding match and route, go next binding with new dest
			 { _, false, _ } -> get_destinations (X, Headers, R, GOT, [Dest | Dests])
		    end
		end;
        [#headers_bindings{destination=Dest, binding_type=one, stop_on_match=SOM, gotos={GOT,GOF}, dontroute=DontRoute, cargs=TransformedArgs}] ->
%%io:format("Cu GOT GOF : ~p~p~p~n", [CurrentOrder, GOT, GOF]),
	    case { DontRoute, SOM, lists:member(Dest, Dests) } of
		%% if destination is already matched, go next binding
		{ _, _, true } -> get_destinations (X, Headers, R, GotoOrder, Dests);
		%% bad use : do not route and stop anyway, ending with already matched bindings
		{ true, any, _ } -> [Dests];
		_ -> case { headers_match_one(TransformedArgs, Headers, false), DontRoute, SOM } of
			 %% binding dont match and stop, ending with already matched bindings
			 { false, _, any } -> Dests;
			 { false, _, false } -> Dests;
			 %% binding dont match, go next binding
			 { false, _, _ } -> get_destinations (X, Headers, R, GOF, Dests);
			 %% binding match and stop but dont route, ending with already matched bindings
			 { _, true, any } -> Dests;
			 { _, true, true } -> Dests;
			 %% binding match but dont route, go next binding
			 { _, true, _ } -> get_destinations (X, Headers, R, GOT, Dests);
			 %% binding match and stop but route, ending with new dest
			 { _, false, any } -> [Dest | Dests];
		         { _, false, true } -> [Dest | Dests];
			 %% binding match and route, go next binding with new dest
			 { _, false, _ } -> get_destinations (X, Headers, R, GOT, [Dest | Dests])
		    end
		end;
        %% Binding type is any
        [#headers_bindings{destination=Dest, binding_type=any, last_nxkey=LNXK, stop_on_match=SOM, gotos={GOT,GOF}, dontroute=DontRoute, cargs=TransformedArgs}] ->
%%io:format("Cu GOT GOF : ~p~p~p~n", [CurrentOrder, GOT, GOF]),
	    case { DontRoute, SOM, lists:member(Dest, Dests) } of
		%% if destination is already matched, go next binding
		{ _, _, true } -> get_destinations (X, Headers, R, GotoOrder, Dests);
		%% bad use : do not route and stop anyway, ending with already matched bindings
		{ true, any, _ } -> [Dests];
		_ -> case { headers_match_any(TransformedArgs, Headers, LNXK), DontRoute, SOM } of
			 %% binding dont match and stop, ending with already matched bindings
			 { false, _, any } -> Dests;
			 { false, _, false } -> Dests;
			 %% binding dont match, go next binding
			 { false, _, _ } -> get_destinations (X, Headers, R, GOF, Dests);
			 %% binding match and stop but dont route, ending with already matched bindings
			 { _, true, any } -> Dests;
			 { _, true, true } -> Dests;
			 %% binding match but dont route, go next binding
			 { _, true, _ } -> get_destinations (X, Headers, R, GOT, Dests);
			 %% binding match and stop but route, ending with new dest
			 { _, false, any } -> [Dest | Dests];
		         { _, false, true } -> [Dest | Dests];
			 %% binding match and route, go next binding with new dest
			 { _, false, _ } -> get_destinations (X, Headers, R, GOT, [Dest | Dests])
		    end
		end
    end.

default_match_order() -> 2000.

get_match_order(Args) ->
    case rabbit_misc:table_lookup(Args, <<"x-match-order">>) of
        {long, Order} -> Order;
	_ -> default_match_order()
    end.

%% Is called only for new bindings to create
validate_binding(_X, #binding{args = Args}) ->
    case rabbit_misc:table_lookup(Args, <<"x-match">>) of
        {longstr, <<"all">>} -> validate_binding(Args, xmatchorder);
        {longstr, <<"any">>} -> validate_binding(Args, xmatchorder);
        {longstr, <<"one">>} -> validate_binding(Args, xmatchorder);
        {longstr, Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field value ~p; "
                                  "expected all or any", [Other]}};
        {Type,    Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field type ~p (value ~p); "
                                  "expected longstr", [Type, Other]}};
        undefined            -> validate_binding(Args, xmatchorder)
    end;
validate_binding(Args, xmatchorder) ->
    case rabbit_misc:table_lookup(Args, <<"x-match-order">>) of
        {long, N} when is_number(N) -> validate_binding(Args, xmatchdontroute);
        {Type, Other} -> {error, {binding_invalid,
                        "Invalid x-match-order field type ~p (value ~p); "
                        "expected long number", [Type, Other]}};
        undefined -> validate_binding(Args, xmatchdontroute)
    end;
validate_binding(Args, xmatchdontroute) ->
    case rabbit_misc:table_lookup(Args, <<"x-match-dontroute">>) of
        {bool, true} -> ok;
        {Type, Other} -> {error, {binding_invalid,
                         "Invalid x-match-dontroute field type ~p (value ~p); "
                         "expected bool and true only", [Type, Other]}};
        undefined ->ok
    end.



%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%% REQUIRES BOTH PATTERN AND DATA TO BE SORTED ASCENDING BY KEY.
%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

% No more binding header to match with, return false
headers_match_any([], _, _) -> false;
% No more data and no nx op, return false
headers_match_any([_], [], nonx) -> false;
% Last nx op is after (or is) current key, return true
headers_match_any([{PK, _, _} | _], [], LNXK)
    when PK =< LNXK -> true;
% There won't be other nx op in bindings, return false
headers_match_any([{PK, _, _} | _], [], LNXK)
    when PK > LNXK -> false;

% Go next data to match current binding key
headers_match_any(P = [{PK, _, _} | _], [{DK, _, _} | DRest], LNXK)
    when PK > DK -> headers_match_any(P, DRest, LNXK);
% Current binding key must not exist in data, return true
headers_match_any([{PK, nx,_} | _], [{DK, _, _} | _], _)
    when PK < DK -> true;
% Current binding key does not exist in data, go next binding key
headers_match_any([{PK, _, _} | PRest], D = [{DK, _, _} | _], LNXK)
    when PK < DK -> headers_match_any(PRest, D, LNXK);
% ---------------------
% From here, PK == DK :
% ---------------------
headers_match_any([{_, eq, PV} | _], [{_, _, DV} | _], _) when DV == PV -> true;
headers_match_any([{_, ex,_} | _], _, _) -> true;
headers_match_any([{_, ne, PV} | _], [{_, _, DV} | _], _) when DV /= PV -> true;
headers_match_any([{_, gt, PV} | _], [{_, _, DV} | _], _) when DV > PV -> true;
headers_match_any([{_, ge, PV} | _], [{_, _, DV} | _], _) when DV >= PV -> true;
headers_match_any([{_, lt, PV} | _], [{_, _, DV} | _], _) when DV < PV -> true;
headers_match_any([{_, le, PV} | _], [{_, _, DV} | _], _) when DV =< PV -> true;
% No match, go next binding
headers_match_any([_ | PRest], D, LNXK) ->
    headers_match_any(PRest, D, LNXK).


headers_match_one([], _, Result) -> Result;
% No more data; nx is the only op we must care
headers_match_one([{_, nx,_} | _], [], true) -> false;
headers_match_one([{_, nx,_} | PRest], [], false) ->
    headers_match_one(PRest, [], true);
% No more data; go next binding
headers_match_one([_ | PRest], [], Result) ->
    headers_match_one(PRest, [], Result);
% Go next data to match current binding key
headers_match_one(P = [{PK, _, _} | _], [{DK, _, _} | DRest], Result)
    when PK > DK -> headers_match_one(P, DRest, Result);
% Current binding key must not exist in data
headers_match_one([{PK, nx,_} | _], [{DK, _, _} | _], true)
    when PK < DK -> false;
headers_match_one([{PK, nx,_} | PRest], D = [{DK, _, _} | _], false)
    when PK < DK -> headers_match_one(PRest, D, true);
% Current binding key does not exist in data, go next binding key
headers_match_one([{PK, _, _} | PRest], D = [{DK, _, _} | _], Result)
    when PK < DK -> headers_match_one(PRest, D, Result);
% ---------------------
% From here, PK == DK :
% ---------------------
headers_match_one([{_, eq, PV} | _], [{_, _, DV} | _], true) when DV == PV -> false;
headers_match_one([{_, eq, PV} | PRest], [{_, _, DV} | DRest], false) when DV == PV ->
     headers_match_one(PRest, DRest, true);
headers_match_one([{_, ex,_} | _], _, true) -> false;
headers_match_one([{_, ex,_} | PRest], D, false) ->
    headers_match_one(PRest, D, true);
headers_match_one([{_, ne, PV} | _], [{_, _, DV} | _], true) when DV /= PV -> false;
headers_match_one([{_, ne, PV} | PRest], D = [{_, _, DV} | _], false) when DV /= PV ->
    headers_match_one(PRest, D, true);
headers_match_one([{_, gt, PV} | _], [{_, _, DV} | _], true) when DV > PV -> false;
headers_match_one([{_, gt, PV} | PRest], D = [{_, _, DV} | _], false) when DV > PV ->
    headers_match_one(PRest, D, true);
headers_match_one([{_, ge, PV} | _], [{_, _, DV} | _], true) when DV >= PV -> true;
headers_match_one([{_, ge, PV} | PRest], D = [{_, _, DV} | _], false) when DV >= PV ->
    headers_match_one(PRest, D, true);
headers_match_one([{_, lt, PV} | _], [{_, _, DV} | _], true) when DV < PV -> true;
headers_match_one([{_, lt, PV} | PRest], D = [{_, _, DV} | _], false) when DV < PV ->
    headers_match_one(PRest, D, true);
headers_match_one([{_, le, PV} | _], [{_, _, DV} | _], true) when DV =< PV -> true;
headers_match_one([{_, le, PV} | PRest], D = [{_, _, DV} | _], false) when DV =< PV ->
    headers_match_one(PRest, D, true);
% No match, go next binding
headers_match_one([_ | PRest], D, Result) ->
    headers_match_one(PRest, D, Result).





% No more binding header to match with, return true
headers_match_all([], _, _) -> true;
% No more data and no nx op, return false
headers_match_all([_], [], nonx) -> false;
% Purge nx op on no data as all these are true
headers_match_all([{_, nx, _} | PRest], [], NX) ->
    headers_match_all(PRest, [], NX);
% No more data with some op other than nx, return false
headers_match_all([_], [], _) -> false;

% Current data key is not in binding, go next data
headers_match_all(P = [{PK, _, _} | _], [{DK, _, _} | DRest], NX)
    when PK > DK -> headers_match_all(P, DRest, NX);
% Current binding key must not exist in data, go next binding
headers_match_all([{PK, nx, _} | PRest], D = [{DK, _, _} | _], NX)
    when PK < DK -> headers_match_all(PRest, D, NX);
% Current binding key does not exist in data, return false
headers_match_all([{PK, _, _} | _], [{DK, _, _} | _], _)
    when PK < DK -> false;
% ---------------------
% From here, PK == DK :
% ---------------------
% WARNS : do not "x-?ex n" AND "x-?* n" it does not work !
% If key must exists go next
headers_match_all([{_, ex,_} | PRest], [ _ | DRest], NX) ->
    headers_match_all(PRest, DRest, NX);
% else if values must match and it matches then go next..
headers_match_all([{_, eq, PV} | PRest], [{_, _, DV} | DRest], NX)
    when PV == DV -> headers_match_all(PRest, DRest, NX);
headers_match_all([{_, eq, _} | _], _, _) -> false;
% Key must not exist, return false
headers_match_all([{_, nx,_} | _], _, _) -> false;
headers_match_all([{_, ne, PV} | PRest], D = [{_, _, DV} | _], NX)
    when PV /= DV -> headers_match_all(PRest, D, NX);
headers_match_all([{_, ne, _} | _], _, _) -> false;
headers_match_all([{_, gt, PV} | PRest], D = [{_, _, DV} | _], NX)
    when DV > PV -> headers_match_all(PRest, D, NX);
headers_match_all([{_, gt, _} | _], _, _) -> false;
headers_match_all([{_, ge, PV} | PRest], D = [{_, _, DV} | _], NX)
    when DV >= PV -> headers_match_all(PRest, D, NX);
headers_match_all([{_, ge, _} | _], _, _) -> false;
headers_match_all([{_, lt, PV} | PRest], D = [{_, _, DV} | _], NX)
    when DV < PV -> headers_match_all(PRest, D, NX);
headers_match_all([{_, lt, _} | _], _, _) -> false;
headers_match_all([{_, le, PV} | PRest], D = [{_, _, DV} | _], NX)
    when DV =< PV -> headers_match_all(PRest, D, NX);
headers_match_all([{_, le, _} | _], _, _) -> false.



%% Flatten one level for list of values (array)
flatten_bindings_args(Args) ->
	flatten_bindings_args(Args, []).

flatten_bindings_args([], Result) -> Result;
flatten_bindings_args ([ {<<"x-?ex">>, array, Vs} | R ], Result) ->
	Res = [ { <<"x-?ex ", K/binary>>, long, 0 } || {_, K} <- Vs ],
	flatten_bindings_args (R, lists:append ([ Res , Result ]));
flatten_bindings_args ([ {<<"x-?nx">>, array, Vs} | R ], Result) ->
	Res = [ { <<"x-?nx ", K/binary>>, long, 0 } || {_, K} <- Vs ],
	flatten_bindings_args (R, lists:append ([ Res , Result ]));
flatten_bindings_args ([ {K, array, Vs} | R ], Result) ->
	Res = [ { K, T, V } || {T, V} <- Vs ],
	flatten_bindings_args (R, lists:append ([ Res , Result ]));
flatten_bindings_args ([ {K, T, V} | R ], Result) ->
	flatten_bindings_args (R, [ {K, T, V} | Result ]).
	

%% Delete x-* keys and ignore types excepted "void" used to match existence
transform_binding_args(Args) -> transform_binding_args(Args, [], all, default_match_order(), nonx, undefined, ?DEFAULT_GOTO_ORDER, ?DEFAULT_GOTO_ORDER, false).

transform_binding_args([], Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute) -> { Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute };

transform_binding_args([ {K, void, _V} | R ], Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, [ {K, ex,0} | Result], BT, Order, LNXK, SOM, GOT, GOF, DontRoute);
transform_binding_args([ {<<"x-?ex ", K/binary>>, _T, _V} | R ], Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, [ {K, ex,0} | Result], BT, Order, LNXK, SOM, GOT, GOF, DontRoute);

transform_binding_args([ {<<"x-?nx ", K/binary>>, _T, _V} | R ], Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute)
    when K > LNXK ->
    transform_binding_args (R, [ {K, nx,0} | Result], BT, Order, K, SOM, GOT, GOF, DontRoute);
transform_binding_args([ {<<"x-?nx ", K/binary>>, _T, _V} | R ], Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, [ {K, nx,0} | Result], BT, Order, LNXK, SOM, GOT, GOF, DontRoute);

transform_binding_args([ {<<"x-?gt ", K/binary>>, _T, V} | R ], Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, [ {K, gt, V} | Result], BT, Order, LNXK, SOM, GOT, GOF, DontRoute);
transform_binding_args([ {<<"x-?ge ", K/binary>>, _T, V} | R ], Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, [ {K, ge, V} | Result], BT, Order, LNXK, SOM, GOT, GOF, DontRoute);
transform_binding_args([ {<<"x-?lt ", K/binary>>, _T, V} | R ], Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, [ {K, lt, V} | Result], BT, Order, LNXK, SOM, GOT, GOF, DontRoute);
transform_binding_args([ {<<"x-?le ", K/binary>>, _T, V} | R ], Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, [ {K, le, V} | Result], BT, Order, LNXK, SOM, GOT, GOF, DontRoute);
transform_binding_args([ {<<"x-?eq ", K/binary>>, _T, V} | R ], Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, [ {K, eq, V} | Result], BT, Order, LNXK, SOM, GOT, GOF, DontRoute);
transform_binding_args([ {<<"x-?ne ", K/binary>>, _T, V} | R ], Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, [ {K, ne, V} | Result], BT, Order, LNXK, SOM, GOT, GOF, DontRoute);

transform_binding_args([{<<"x-match">>, longstr, <<"any">>} | R], Result, _, Order, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, Result, any, Order, LNXK, SOM, GOT, GOF, DontRoute);
transform_binding_args([{<<"x-match">>, longstr, <<"all">>} | R], Result, _, Order, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, Result, all, Order, LNXK, SOM, GOT, GOF, DontRoute);
transform_binding_args([{<<"x-match">>, longstr, <<"one">>} | R], Result, _, Order, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, Result, one, Order, LNXK, SOM, GOT, GOF, DontRoute);

transform_binding_args([{<<"x-match-order">>, long, Order} | R], Result, BT, _, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute);

transform_binding_args([{<<"x-match-goto">>, long, N} | R], Result, BT, Order, LNXK, SOM, _, _, DontRoute) ->
    transform_binding_args (R, Result, BT, Order, LNXK, SOM, N, N, DontRoute);
transform_binding_args([{<<"x-match-goto-true">>, long, N} | R], Result, BT, Order, LNXK, SOM, _, GOF, DontRoute) ->
    transform_binding_args (R, Result, BT, Order, LNXK, SOM, N, GOF, DontRoute);
transform_binding_args([{<<"x-match-goto-false">>, long, N} | R], Result, BT, Order, LNXK, SOM, GOT, _, DontRoute) ->
    transform_binding_args (R, Result, BT, Order, LNXK, SOM, GOT, N, DontRoute);

transform_binding_args([{<<"x-match-stop">>, bool, true} | R], Result, BT, Order, LNXK, _, GOT, GOF, DontRoute) ->
    transform_binding_args (R, Result, BT, Order, LNXK, any, GOT, GOF, DontRoute);
transform_binding_args([{<<"x-match-stop-true">>, bool, true} | R], Result, BT, Order, LNXK, _, GOT, GOF, DontRoute) ->
    transform_binding_args (R, Result, BT, Order, LNXK, true, GOT, GOF, DontRoute);
transform_binding_args([{<<"x-match-stop-false">>, bool, true} | R], Result, BT, Order, LNXK, _, GOT, GOF, DontRoute) ->
    transform_binding_args (R, Result, BT, Order, LNXK, false, GOT, GOF, DontRoute);

transform_binding_args([{<<"x-match-dontroute">>, bool, true} | R], Result, BT, Order, LNXK, SOM, GOT, GOF, _) ->
    transform_binding_args (R, Result, BT, Order, LNXK, SOM, GOT, GOF, true);

transform_binding_args([ {<<"x-", _/binary>>, _T, _V} | R ], Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute);
transform_binding_args([ {K, _T, V} | R ], Result, BT, Order, LNXK, SOM, GOT, GOF, DontRoute) ->
    transform_binding_args (R, [ {K, eq, V} | Result], BT, Order, LNXK, SOM, GOT, GOF, DontRoute).


% Store the new "binding id" in rabbit_headers_bindings_keys whose key is X
%  and store new transformed binding headers
add_binding(transaction, X, BindingToAdd = #binding{destination = Dest, args = Args}) ->
    BindingId = crypto:hash(md5,term_to_binary(BindingToAdd)),
    FArgs = flatten_bindings_args(Args),
    { CleanArgs, BindingType, Order, LNXK, SOM, GOT, GOF, DontRoute } = transform_binding_args (FArgs),
    NewR = #headers_bindings_keys{exchange = X, binding_id = {Order,BindingId}},
    mnesia:write (rabbit_headers_bindings_keys, NewR, write),
    XR = #headers_bindings{exch_bind = {X, {Order,BindingId}}, destination = Dest, binding_type = BindingType, last_nxkey = LNXK, stop_on_match = SOM, gotos={GOT,GOF}, dontroute=DontRoute, cargs = rabbit_misc:sort_field_table(CleanArgs)},
    mnesia:write (rabbit_headers_bindings, XR, write),

    % Reorder results by x-match-order because ordered_bag does not exists
    SortedValues = lists:sort (mnesia:read (rabbit_headers_bindings_keys, X)),
    % can't use mnesia:clean_table because of nested transaction ?
    lists:foreach (fun(K) -> mnesia:delete (rabbit_headers_bindings_keys, K, write) end, mnesia:all_keys(rabbit_headers_bindings_keys)),
    lists:foreach (fun(OrderedBinding) -> mnesia:write (rabbit_headers_bindings_keys, OrderedBinding, write) end, SortedValues);
add_binding(_Tx, _X, _B) -> ok.


remove_bindings(transaction, X, Bs) ->
    BindingsIDs_todel = [ {get_match_order(Args), crypto:hash(md5,term_to_binary(Binding)) } || Binding=#binding{args=Args} <- Bs ],

    lists:foreach (fun({Order,BindingID_todel}) -> mnesia:delete ({ rabbit_headers_bindings, { X, {Order, BindingID_todel } } }) end, BindingsIDs_todel),
    lists:foreach (
        fun({Order,BindingID_todel}) ->
            R_todel = #headers_bindings_keys{exchange = X, binding_id = {Order,BindingID_todel}},
            mnesia:delete_object (rabbit_headers_bindings_keys, R_todel, write)
        end, BindingsIDs_todel);
remove_bindings(_Tx, _X, _Bs) -> ok.


validate(_X) -> ok.
create(_Tx, _X) -> ok.
delete(_Tx, _X, _Bs) -> ok.
policy_changed(_X1, _X2) -> ok.
assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).
