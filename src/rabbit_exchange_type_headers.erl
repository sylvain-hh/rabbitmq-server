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
-define(DEFAULT_MATCH_ORDER, 2000).

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
    DDD = get_routes (X, Headers, BindingsIDs, ?DEFAULT_GOTO_ORDER, []),
    DDD.



% Retreive destinations from bindings ids
% No more bindings, returns computed Dests
get_routes (_X, _Headers, [], _GotoOrder, Dests) -> Dests;
get_routes (X, Headers, [ #headers_bindings_keys{binding_id={CurrentOrder,_}} | R ], GotoOrder, Dests) when is_number(GotoOrder) andalso CurrentOrder < GotoOrder ->
    get_routes (X, Headers, R, GotoOrder, Dests);
get_routes (X, Headers, [ #headers_bindings_keys{binding_id=BindingId} | R ], GotoOrder, Dests) ->
    case ets:lookup(rabbit_headers_bindings, {X,BindingId}) of
        %% It may happen that a binding is deleted in the meantime (?)
        [] -> get_routes (X, Headers, R, GotoOrder, Dests);
        %% Binding type is all
        [#headers_bindings{destinations={Dest,[DATS,DAFS,DDTS,DDFS]}, binding_type=all, stop_on_match=SOM, gotos={GOT,GOF}, options={ForceMatch}, cargs=TransformedArgs}] ->
	    case { lists:member(Dest, Dests), ForceMatch } of
		{ true, false } -> get_routes (X, Headers, R, GotoOrder, Dests);
		_ -> case { headers_match_all(TransformedArgs, Headers), SOM } of
			 { true, {1, _} } -> lists:subtract(lists:append([[Dest | Dests], DATS]),DDTS);
			 { false, {_, 1} } -> lists:subtract(lists:append([Dests, DAFS]),DDFS);
			 { true, _ } -> get_routes (X, Headers, R, GOT, lists:subtract(lists:append([[Dest | Dests], DATS]),DDTS));
			 { false, _ } -> get_routes (X, Headers, R, GOF, lists:subtract(lists:append([Dests, DAFS]),DDFS))
		     end
            end;
        %% Binding type is one
        [#headers_bindings{destinations={Dest,[DATS,DAFS,DDTS,DDFS]}, binding_type=one, stop_on_match=SOM, gotos={GOT,GOF}, options={ForceMatch}, cargs=TransformedArgs}] ->
	    case { lists:member(Dest, Dests), ForceMatch } of
		{ true, false } -> get_routes (X, Headers, R, GotoOrder, Dests);
		_ -> case { headers_match_one(TransformedArgs, Headers, false), SOM } of
			 { true, {1, _} } -> lists:subtract(lists:append([[Dest | Dests], DATS]),DDTS);
			 { false, {_, 1} } -> lists:subtract(lists:append([Dests, DAFS]),DDFS);
			 { true, _ } -> get_routes (X, Headers, R, GOT, lists:subtract(lists:append([[Dest | Dests], DATS]),DDTS));
			 { false, _ } -> get_routes (X, Headers, R, GOF, lists:subtract(lists:append([Dests, DAFS]),DDFS))
		     end
            end;
        %% Binding type is any
        [#headers_bindings{destinations={Dest,[DATS,DAFS,DDTS,DDFS]}, binding_type=any, stop_on_match=SOM, gotos={GOT,GOF}, options={ForceMatch}, cargs=TransformedArgs}] ->
	    case { lists:member(Dest, Dests), ForceMatch } of
		{ true, false } -> get_routes (X, Headers, R, GotoOrder, Dests);
		_ -> case { headers_match_any(TransformedArgs, Headers), SOM } of
			 { true, {1, _} } -> lists:subtract(lists:append([[Dest | Dests], DATS]),DDTS);
			 { false, {_, 1} } -> lists:subtract(lists:append([Dests, DAFS]),DDFS);
			 { true, _ } -> get_routes (X, Headers, R, GOT, lists:subtract(lists:append([[Dest | Dests], DATS]),DDTS));
			 { false, _ } -> get_routes (X, Headers, R, GOF, lists:subtract(lists:append([Dests, DAFS]),DDFS))
		     end
            end
    end.


get_match_order(Args) ->
    case rabbit_misc:table_lookup(Args, <<"x-match-order">>) of
        {long, Order} -> Order;
	_ -> ?DEFAULT_MATCH_ORDER
    end.
get_match_force(Args) ->
    case rabbit_misc:table_lookup(Args, <<"x-match-force">>) of
        {boolean, true} -> true;
	_ -> false
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
                                  "expected all or any or one", [Other]}};
        {Type,    Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field type ~p (value ~p); "
                                  "expected longstr", [Type, Other]}};
        undefined            -> validate_binding(Args, xmatchorder)
    end;
validate_binding(Args, xmatchorder) ->
    case rabbit_misc:table_lookup(Args, <<"x-match-order">>) of
        {long, N} when is_number(N) -> ok;
        {Type, Other} -> {error, {binding_invalid,
                        "Invalid x-match-order field type ~p (value ~p); "
                        "expected long number", [Type, Other]}};
        undefined -> ok
    end.


validate_binding_args(Args) ->
        validate_binding_args(Args, [], all).
%% No more args, return result
validate_binding_args([], Result)
    -> Result;
validate_binding_args ([ {K, array, Vs} | NextArg ], Result) ->
	Res = [ { K, T, V } || {T, V} <- Vs ],
	validate_binding_args (NextArg, lists:append ([ Res , Result ]));
validate_binding_args ([ {K, T, V} | NextArg ], Result) ->
	validate_binding_args (NextArg, [ {K, T, V} | Result ]).



validate_binding_args_check_keys_uniqueness (Args) ->
	Keys = [K || {K,_,_} <- Args],
	DistinctKeys = sets:to_list (sets:from_list (Keys)),
	DuplicatedKeysStr = string:join (lists:subtract (DistinctKeys, Keys), ", "),
	case DuplicatedKeysStr =:= "" of
		true -> ok;
		{error, {binding_invalid, "Multiple definition of key(s) ~p", [DuplicatedKeysStr]} }
	end.

validate_binding_args_check_exclusive_keys (Args, ExcludedKeys, ErrorMessage) ->
	Keys = [K || {K,_,_} <- Args],
	case lists:subtract (ExcludedKeys, Keys) of
		[] -> ok;
		{error, {binding_invalid, ErrorMessage, []} }
	end.


%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%% REQUIRES BOTH PATTERN AND DATA TO BE SORTED ASCENDING BY KEY.
%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

% No more binding header to match with, return false
headers_match_any([], _) -> false;
% On no data left, only nx operator can return true
headers_match_any([{_, nx, _} | _], []) -> true;
headers_match_any([_ | BNext], []) ->
    headers_match_any(BNext, []);
% Go next data to match current binding key
headers_match_any(BCur = [{BK, _, _} | _], [{DK, _, _} | DNext])
    when BK > DK -> headers_match_any(BCur, DNext);
% nx operator : current binding key must not exist in data, return true
headers_match_any([{BK, nx, _} | _], [{DK, _, _} | _])
    when BK < DK -> true;
% Current binding key does not exist in data, go next binding key
headers_match_any([{BK, _, _} | BNext], DCur = [{DK, _, _} | _])
    when BK < DK -> headers_match_any(BNext, DCur);
% ---------------------
% From here, BK == DK :
% ---------------------
headers_match_any([{_, eq, BV} | _], [{_, _, DV} | _]) when DV == BV -> true;
headers_match_any([{_, ex, _} | _], _) -> true;
headers_match_any([{_, ne, BV} | _], [{_, _, DV} | _]) when DV /= BV -> true;
headers_match_any([{_, gt, BV} | _], [{_, _, DV} | _]) when DV > BV -> true;
headers_match_any([{_, ge, BV} | _], [{_, _, DV} | _]) when DV >= BV -> true;
headers_match_any([{_, lt, BV} | _], [{_, _, DV} | _]) when DV < BV -> true;
headers_match_any([{_, le, BV} | _], [{_, _, DV} | _]) when DV =< BV -> true;
% No match, go next binding
headers_match_any([_ | BNext], DCur) ->
    headers_match_any(BNext, DCur).


% Initial call set Result to false; that Result must switch once to true
%  and must stay to be true. A true match with Result to true does match false.
headers_match_one([], _, Result) -> Result;
% No more data; nx is the only op we must care
headers_match_one([{_, nx, _} | _], [], true) -> false;
headers_match_one([{_, nx, _} | BNext], [], false) ->
    headers_match_one(BNext, [], true);
headers_match_one([_ | BNext], [], Result) ->
    headers_match_one(BNext, [], Result);
% Go next data to match current binding key
headers_match_one(BCur = [{BK, _, _} | _], [{DK, _, _} | DNext], Result)
    when BK > DK -> headers_match_one(BCur, DNext, Result);
% Current binding key must not exist in data
headers_match_one([{BK, nx, _} | _], [{DK, _, _} | _], true)
    when BK < DK -> false;
headers_match_one([{BK, nx, _} | BNext], DCur = [{DK, _, _} | _], false)
    when BK < DK -> headers_match_one(BNext, DCur, true);
% Current binding key does not exist in data, go next binding key
headers_match_one([{BK, _, _} | BNext], DCur = [{DK, _, _} | _], Result)
    when BK < DK -> headers_match_one(BNext, DCur, Result);
% ---------------------
% From here, BK == DK :
% ---------------------
headers_match_one([{_, eq, BV} | _], [{_, _, DV} | _], true) when DV == BV -> false;
headers_match_one([{_, eq, BV} | BNext], DCur = [{_, _, DV} | _], false) when DV == BV ->
     headers_match_one(BNext, DCur, true);
headers_match_one([{_, ex, _} | _], _, true) -> false;
headers_match_one([{_, ex, _} | BNext], [_ | DNext], false) ->
    headers_match_one(BNext, DNext, true);
headers_match_one([{_, ne, BV} | _], [{_, _, DV} | _], true) when DV /= BV -> false;
headers_match_one([{_, ne, BV} | BNext], DCur = [{_, _, DV} | _], false) when DV /= BV ->
    headers_match_one(BNext, DCur, true);
headers_match_one([{_, gt, BV} | _], [{_, _, DV} | _], true) when DV > BV -> false;
headers_match_one([{_, gt, BV} | BNext], DCur = [{_, _, DV} | _], false) when DV > BV ->
    headers_match_one(BNext, DCur, true);
headers_match_one([{_, ge, BV} | _], [{_, _, DV} | _], true) when DV >= BV -> true;
headers_match_one([{_, ge, BV} | BNext], DCur = [{_, _, DV} | _], false) when DV >= BV ->
    headers_match_one(BNext, DCur, true);
headers_match_one([{_, lt, BV} | _], [{_, _, DV} | _], true) when DV < BV -> true;
headers_match_one([{_, lt, BV} | BNext], DCur = [{_, _, DV} | _], false) when DV < BV ->
    headers_match_one(BNext, DCur, true);
headers_match_one([{_, le, BV} | _], [{_, _, DV} | _], true) when DV =< BV -> true;
headers_match_one([{_, le, BV} | BNext], DCur = [{_, _, DV} | _], false) when DV =< BV ->
    headers_match_one(BNext, DCur, true);
% No match, go next binding
headers_match_one([_ | BNext], DCur, Result) ->
    headers_match_one(BNext, DCur, Result).





% No more binding header to match with, return true
headers_match_all([], _) -> true;
% Purge nx op on no data as all these are true
headers_match_all([{_, nx, _} | BNext], []) ->
    headers_match_all(BNext, []);
headers_match_all(_, []) -> false;

% Current data key is not in binding, go next data
headers_match_all(BCur = [{BK, _, _} | _], [{DK, _, _} | DNext])
    when BK > DK -> headers_match_all(BCur, DNext);
% Current binding key must not exist in data, go next binding
headers_match_all([{BK, nx, _} | BNext], DCur = [{DK, _, _} | _])
    when BK < DK -> headers_match_all(BNext, DCur);
% Current binding key does not exist in data, return false
headers_match_all([{BK, _, _} | _], [{DK, _, _} | _])
    when BK < DK -> false;
% ---------------------
% From here, BK == DK :
% ---------------------
headers_match_all([{_, eq, PV} | BNext], [{_, _, DV} | DNext])
    when PV == DV -> headers_match_all(BNext, DNext);
headers_match_all([{_, eq, _} | _], _) -> false;
% Key must not exist, return false
headers_match_all([{_, nx, _} | _], _) -> false;
headers_match_all([{_, ex, _} | BNext], [ _ | DNext]) ->
    headers_match_all(BNext, DNext);
headers_match_all([{_, ne, PV} | BNext], DCur = [{_, _, DV} | _])
    when PV /= DV -> headers_match_all(BNext, DCur);
headers_match_all([{_, ne, _} | _], _) -> false;
headers_match_all([{_, gt, PV} | BNext], DCur = [{_, _, DV} | _])
    when DV > PV -> headers_match_all(BNext, DCur);
headers_match_all([{_, gt, _} | _], _) -> false;
headers_match_all([{_, ge, PV} | BNext], DCur = [{_, _, DV} | _])
    when DV >= PV -> headers_match_all(BNext, DCur);
headers_match_all([{_, ge, _} | _], _) -> false;
headers_match_all([{_, lt, PV} | BNext], DCur = [{_, _, DV} | _])
    when DV < PV -> headers_match_all(BNext, DCur);
headers_match_all([{_, lt, _} | _], _) -> false;
headers_match_all([{_, le, PV} | BNext], DCur = [{_, _, DV} | _])
    when DV =< PV -> headers_match_all(BNext, DCur);
headers_match_all([{_, le, _} | _], _) -> false.



%% Flatten one level for list of values (array)
flatten_bindings_args(Args) ->
	flatten_bindings_args(Args, []).

flatten_bindings_args([], Result) -> Result;
flatten_bindings_args ([ {K, array, Vs} | R ], Result) ->
	Res = [ { K, T, V } || {T, V} <- Vs ],
	flatten_bindings_args (R, lists:append ([ Res , Result ]));
flatten_bindings_args ([ {K, T, V} | R ], Result) ->
	flatten_bindings_args (R, [ {K, T, V} | Result ]).
	

%% DATS : Destinations to Add on True Set
%% DAFS : Destinations to Add on False Set
%% DDTS : Destinations to Del on True Set
%% DDFS : Destinations to Del on False Set
transform_binding_args_dests(VHost, BindingArgs) ->
    [DATS,DAFS,DDTS,DDFS] = transform_binding_args_dests(VHost, BindingArgs, sets:new(), sets:new(), sets:new(), sets:new()),
    [ sets:to_list(DATS), sets:to_list(DAFS), sets:to_list(DDTS), sets:to_list(DDFS) ].

transform_binding_args_dests(_, [], DATS,DAFS,DDTS,DDFS) -> [ DATS,DAFS,DDTS,DDFS ];
transform_binding_args_dests(VHost, [ {<<"x-match-addq-ontrue">>, longstr, V} | R ], DATS,DAFS,DDTS,DDFS) ->
    D = rabbit_misc:r(VHost, queue, V),
    transform_binding_args_dests (VHost, R, sets:add_element(D,DATS), DAFS, DDTS,DDFS);
transform_binding_args_dests(VHost, [ {<<"x-match-adde-ontrue">>, longstr, V} | R ], DATS,DAFS,DDTS,DDFS) ->
    D = rabbit_misc:r(VHost, exchange, V),
    transform_binding_args_dests (VHost, R, sets:add_element(D,DATS), DAFS, DDTS,DDFS);
transform_binding_args_dests(VHost, [ {<<"x-match-addq-onfalse">>, longstr, V} | R ], DATS,DAFS,DDTS,DDFS) ->
    D = rabbit_misc:r(VHost, queue, V),
    transform_binding_args_dests (VHost, R, DATS, sets:add_element(D,DAFS), DDTS,DDFS);
transform_binding_args_dests(VHost, [ {<<"x-match-adde-onfalse">>, longstr, V} | R ], DATS,DAFS,DDTS,DDFS) ->
    D = rabbit_misc:r(VHost, exchange, V),
    transform_binding_args_dests (VHost, R, DATS, sets:add_element(D,DAFS), DDTS,DDFS);
transform_binding_args_dests(VHost, [ {<<"x-match-delq-ontrue">>, longstr, V} | R ], DATS,DAFS,DDTS,DDFS) ->
    D = rabbit_misc:r(VHost, queue, V),
    transform_binding_args_dests (VHost, R, DATS,DAFS, sets:add_element(D,DDTS), DDFS);
transform_binding_args_dests(VHost, [ {<<"x-match-dele-ontrue">>, longstr, V} | R ], DATS,DAFS,DDTS,DDFS) ->
    D = rabbit_misc:r(VHost, exchange, V),
    transform_binding_args_dests (VHost, R, DATS,DAFS, sets:add_element(D,DDTS), DDFS);
transform_binding_args_dests(VHost, [ {<<"x-match-delq-onfalse">>, longstr, V} | R ], DATS,DAFS,DDTS,DDFS) ->
    D = rabbit_misc:r(VHost, queue, V),
    transform_binding_args_dests (VHost, R, DATS,DAFS, DDTS, sets:add_element(D,DDFS));
transform_binding_args_dests(VHost, [ {<<"x-match-dele-onfalse">>, longstr, V} | R ], DATS,DAFS,DDTS,DDFS) ->
    D = rabbit_misc:r(VHost, exchange, V),
    transform_binding_args_dests (VHost, R, DATS,DAFS, DDTS, sets:add_element(D,DDFS));
transform_binding_args_dests(VHost, [ _ | R ], DATS,DAFS,DDTS,DDFS) ->
    transform_binding_args_dests (VHost, R, DATS,DAFS,DDTS,DDFS).


% Returns the "compiled form" to be stored in mnesia of bindings args related to operators
transform_binding_args_operators([], Res) -> Res;
% void type for key K is the same as "key K must exists"
transform_binding_args_operators([ {K, void, _V} | N ], Res) ->
    transform_binding_args_operators (N, [ {K, ex,0} | Res]);
% key K must exists
%TODO WHY THE HELL THE EXTRA 0 tuple item ???
transform_binding_args_operators([ {<<"x-?ex">>, longstr, K} | N ], Res) ->
    transform_binding_args_operators (N, [ {K, ex,0} | Res]);
% key K must NOT exists
transform_binding_args_operators([ {<<"x-?nx">>, longstr, K} | N ], Res) ->
    transform_binding_args_operators (N, [ {K, nx,0} | Res]);
% value of key K must be less than V or equal to V
transform_binding_args_operators([ {<<"x-?le ", K/binary>>, _, V} | N ], Res) ->
    transform_binding_args_operators (N, [ {K, le, V} | Res]);
% value of key K must be less than V
transform_binding_args_operators([ {<<"x-?lt ", K/binary>>, _, V} | N ], Res) ->
    transform_binding_args_operators (N, [ {K, lt, V} | Res]);
% value of key K must be greater than V
transform_binding_args_operators([ {<<"x-?gt ", K/binary>>, _, V} | N ], Res) ->
    transform_binding_args_operators (N, [ {K, gt, V} | Res]);
% value of key K must be greater than V or equal to V
transform_binding_args_operators([ {<<"x-?ge ", K/binary>>, _, V} | N ], Res) ->
    transform_binding_args_operators (N, [ {K, ge, V} | Res]);
% value of key K must be equal to V
transform_binding_args_operators([ {<<"x-?eq ", K/binary>>, _, V} | N ], Res) ->
    transform_binding_args_operators (N, [ {K, eq, V} | Res]);
% value of key K must NOT be equal to V
transform_binding_args_operators([ {<<"x-?ne ", K/binary>>, _, V} | N ], Res) ->
    transform_binding_args_operators (N, [ {K, ne, V} | Res]);
% not intersted in x-* keys..
transform_binding_args_operators([ {<<"x-", _/binary>>, _, _} | N ], Res) ->
    transform_binding_args_operators (N, Res);
% for all other cases, value of key K must be equal to V
transform_binding_args_operators([ {K, _, V} | N ], Res) ->
    transform_binding_args_operators (N, [ {K, eq, V} | Res]).



% Returns data to be stored in mnesia of bindings args related to binding type, x-match-goto logic and x-match-stop logic
% Binding type 'all' by default
transform_binding_args(Args) -> transform_binding_args(Args, all, {_STOPONTRUE=0, _STOPONFALSE=0}, ?DEFAULT_GOTO_ORDER, ?DEFAULT_GOTO_ORDER).

transform_binding_args([], BT, SOM, GOT, GOF) -> { BT, SOM, GOT, GOF };


% find binding type if specified; has been defaulted to 'all' in a later call
transform_binding_args([{<<"x-match">>, longstr, <<"any">>} | R], _, SOM, GOT, GOF) ->
    transform_binding_args (R, any, SOM, GOT, GOF);
transform_binding_args([{<<"x-match">>, longstr, <<"all">>} | R], _, SOM, GOT, GOF) ->
    transform_binding_args (R, all, SOM, GOT, GOF);
transform_binding_args([{<<"x-match">>, longstr, <<"one">>} | R], _, SOM, GOT, GOF) ->
    transform_binding_args (R, one, SOM, GOT, GOF);

% x-match-goto-*
%TODO supprimer x-match-goto seul dans l'aide du management
transform_binding_args([{<<"x-match-goto-ontrue">>, long, N} | R], BT, SOM, _, GOF) ->
    transform_binding_args (R, BT, SOM, N, GOF);
transform_binding_args([{<<"x-match-goto-onfalse">>, long, N} | R], BT, SOM, GOT, _) ->
    transform_binding_args (R, BT, SOM, GOT, N);

% x-match-stop-*
%TODO modifier la dependance management pour x-match-stop (non seul et chaine vide)
transform_binding_args([{<<"x-match-stop-ontrue">>, longstr, <<"">>} | R], BT, {_, STOPONFALSE}, GOT, GOF) ->
    transform_binding_args (R, BT, {1, STOPONFALSE}, GOT, GOF);
transform_binding_args([{<<"x-match-stop-onfalse">>, longstr, <<"">>} | R], BT, {STOPONTRUE, _}, GOT, GOF) ->
    transform_binding_args (R, BT, {STOPONTRUE, 1}, GOT, GOF);

% ELSE go to next arg
transform_binding_args([ _ | R ], BT, SOM, GOT, GOF) ->
    transform_binding_args (R, BT, SOM, GOT, GOF).


% Store the new "binding id" in rabbit_headers_bindings_keys whose key is X
%  and store new transformed binding headers
%add_binding(transaction, X = #exchange{name = #resource{virtual_host = VHost}, _='_'}, BindingToAdd = #binding{destination = Dest, args = Args}) ->
add_binding(transaction, #exchange{name = #resource{virtual_host = VHost}} = X, BindingToAdd = #binding{destination = Dest, args = Args}) ->
    BindingId = crypto:hash(md5,term_to_binary(BindingToAdd)),
    Order = get_match_order(Args),
    ForceMatch = get_match_force(Args),
    FArgs = flatten_bindings_args(Args),
    DestsOptions = transform_binding_args_dests(VHost, FArgs),
    CleanArgs = transform_binding_args_operators (FArgs, []),
    { BindingType, SOM, GOT, GOF } = transform_binding_args (FArgs),
    NewR = #headers_bindings_keys{exchange = X, binding_id = {Order,BindingId}},
    ok = mnesia:write (rabbit_headers_bindings_keys, NewR, write),
    XR = #headers_bindings{exch_bind = {X, {Order,BindingId}}, destinations = {Dest,DestsOptions}, binding_type = BindingType, stop_on_match = SOM, gotos={GOT,GOF}, options={ForceMatch}, cargs = rabbit_misc:sort_field_table(CleanArgs)},
    ok = mnesia:write (rabbit_headers_bindings, XR, write),

    %% Because ordered_bag does not exist, we need to reorder bindings
    %%  so that we don't need to sort them in route/2
    OrderedBindings = lists:sort (mnesia:read (rabbit_headers_bindings_keys, X)),
    lists:foreach (fun(R) -> mnesia:delete_object (rabbit_headers_bindings_keys, R, write) end, OrderedBindings),
    lists:foreach (fun(R) -> ok = mnesia:write (rabbit_headers_bindings_keys, R, write) end, OrderedBindings);
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
