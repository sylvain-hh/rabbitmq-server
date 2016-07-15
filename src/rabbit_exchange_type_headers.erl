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


route(X, #delivery{message = #basic_message{content = Content}}) ->
    Headers = case (Content#content.properties)#'P_basic'.headers of
        undefined -> [];
        H         -> rabbit_misc:sort_field_table(H)
    end,
    BindingsIDs = ets:lookup(rabbit_headers_bindings_keys, X),
    get_destinations (X, Headers, BindingsIDs, []).



% Retreive destinations from bindings ids
get_destinations (_X, _Headers, [], Dests) -> Dests;
get_destinations (X, Headers, [ #headers_bindings_keys{binding_id=BindingId} | R ], Dests) ->
    case ets:lookup(rabbit_headers_bindings, {X,BindingId}) of
        %% It may happen that a binding is deleted in the meantime
        [] -> get_destinations (X, Headers, R, Dests);
        %% Binding type is all
% Do we have to care about last_nx_key ??
        [#headers_bindings{destination=Dest, binding_type=all, last_nxkey=_, cargs=TransformedArgs}] ->
            case (false =:= lists:member (Dest, Dests)) andalso headers_match_all(TransformedArgs, Headers) of
                true -> get_destinations (X, Headers, R, [Dest | Dests]);
                _ -> get_destinations (X, Headers, R, Dests)
            end;
        %% Binding type is any
        [#headers_bindings{destination=Dest, binding_type=any, last_nxkey=LNXK, cargs=TransformedArgs}] ->
            case (false =:= lists:member (Dest, Dests)) andalso headers_match_any(TransformedArgs, Headers, LNXK) of
                true -> get_destinations (X, Headers, R, [Dest | Dests]);
                _ -> get_destinations (X, Headers, R, Dests)
            end
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


% What if data headers null ?

%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%% REQUIRES BOTH PATTERN AND DATA TO BE SORTED ASCENDING BY KEY.
%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

% No more binding header to match with, return false
headers_match_any([], _, _) -> false;
% Current op is nx but no more data (means key does not exists), return true
headers_match_any([{_,nx,_} | _], [], _) -> true;
% No more data and no nx op, return false
headers_match_any([_], [], nonx) -> false;
% No more data but "still" nx op, return true
headers_match_any([_], [], _) -> true;
% Go next data to match current binding key
headers_match_any(P = [{PK, _, _} | _], [{DK, _, _} | DRest], LNXK)
    when PK > DK -> io:format("~p ", [?LINE]), headers_match_any(P, DRest, LNXK);
% Current binding key must not exist in data, return true
headers_match_any([{PK, nx,_} | _], [{DK, _, _} | _], _)
    when PK < DK -> true;
% Current binding key does not exist in data, go next binding key
headers_match_any([{PK, _, _} | PRest], D = [{DK, _, _} | _], LNXK)
    when PK < DK -> io:format("~p ", [?LINE]), headers_match_any(PRest, D, LNXK);
% ---------------------
% From here, PK == DK :
% ---------------------
headers_match_any([{_, ex,_} | _], _, _) -> true;
headers_match_any([{_, eq, PV} | _], [{_, _, DV} | _], _) when PV == DV -> true;
headers_match_any([{_, ne, PV} | _], [{_, _, DV} | _], _) when PV /= DV -> true;
headers_match_any([{_, gt, PV} | _], [{_, _, DV} | _], _) when DV > PV -> true;
headers_match_any([{_, ge, PV} | _], [{_, _, DV} | _], _) when DV >= PV -> true;
headers_match_any([{_, lt, PV} | _], [{_, _, DV} | _], _) when DV < PV -> true;
headers_match_any([{_, le, PV} | _], [{_, _, DV} | _], _) when DV =< PV -> true;
headers_match_any([_ | PRest], [_ | DRest], LNXK) -> io:format("~p ", [?LINE]),
    headers_match_any(PRest, DRest, LNXK).

% Binding type is all :
% If there is no header binding nor header data then do match,
headers_match_all([], []) -> io:format("1-",[]), true;
% if there is no header binding then do not match,
headers_match_all([], _) -> io:format("2-",[]), false;
% if there is no data header then do not mtach,
headers_match_all(_, []) -> io:format("3-",[]), false;
% else,
%     if key binding greater than key data then go next,
headers_match_all(P = [{PK, _, _} | _], [{DK, _, _} | DRest])
    when PK > DK -> io:format("4-",[]), headers_match_all(P, DRest);
%     if key binding less than key data then do not match,
headers_match_all([{PK, _, _} | _], [{DK, _, _} | _])
    when PK < DK -> io:format("5-",[]), false;
% From here, we know that PK = DK
% So if key must exists go next..
headers_match_all([{_, ex} | PRest], DRest) ->
    io:format("6-",[]), headers_match_all(PRest, DRest);
% but if key must NOT exists then it don't match..
headers_match_all([{_, nx} | _], _) ->
    io:format("14-",[]), false;
% else if values must match and it matches then go next..
headers_match_all([{_, eq, PV} | PRest], [{_, _, DV} | DRest])
    when PV == DV -> io:format("7-",[]), headers_match_all(PRest, DRest);
% if it don't matches then don't match..
headers_match_all([{_, eq, _} | _], _) -> false;
% but if value must be different then go next..
headers_match_all([{_, ne, PV} | PRest], [{_, _, DV} | DRest])
    when PV /= DV -> io:format("8-",[]), headers_match_all(PRest, DRest);
% else if it match don't match..
headers_match_all([{_, ne, _} | _], _) -> false;
headers_match_all([{_, gt, PV} | PRest], [{_, _, DV} | DRest])
    when DV > PV -> io:format("9-",[]), headers_match_all(PRest, DRest);
headers_match_all([{_, gt, _} | _], _) -> false;
headers_match_all([{_, ge, PV} | PRest], [{_, _, DV} | DRest])
    when DV >= PV -> io:format("10-",[]), headers_match_all(PRest, DRest);
headers_match_all([{_, ge, _} | _], _) -> false;
headers_match_all([{_, lt, PV} | PRest], [{_, _, DV} | DRest])
    when DV < PV -> io:format("11-",[]), headers_match_all(PRest, DRest);
headers_match_all([{_, lt, _} | _], _) -> false;
headers_match_all([{_, le, PV} | PRest], [{_, _, DV} | DRest])
    when DV =< PV -> io:format("12-",[]), headers_match_all(PRest, DRest);
headers_match_all([{_, le, _} | _], _) -> false;
headers_match_all([_ | PRest], [_ | DRest]) ->
    io:format("13-",[]), headers_match_all(PRest, DRest).


%% Delete x-* keys and ignore types excepted "void" used to match existence
transform_binding_args(Args) -> transform_binding_args(Args, [], all, nonx).

transform_binding_args([], Result, BT, LNXK) -> { Result, BT, LNXK };
transform_binding_args([ {K, void, _V} | R ], Result, BT, LNXK) ->
    transform_binding_args (R, [ {K, ex,0} | Result], BT, LNXK);
transform_binding_args([ {<<"x-?ex ", K/binary>>, _T, _V} | R ], Result, BT, LNXK) ->
    transform_binding_args (R, [ {K, ex,0} | Result], BT, LNXK);
transform_binding_args([ {<<"x-?nx ", K/binary>>, _T, _V} | R ], Result, BT, LNXK)
    when K > LNXK ->
    transform_binding_args (R, [ {K, nx,0} | Result], BT, K);
transform_binding_args([ {<<"x-?nx ", K/binary>>, _T, _V} | R ], Result, BT, LNXK) ->
    transform_binding_args (R, [ {K, nx,0} | Result], BT, LNXK);
transform_binding_args([ {<<"x-?gt ", K/binary>>, _T, V} | R ], Result, BT, LNXK) ->
    transform_binding_args (R, [ {K, gt, V} | Result], BT, LNXK);
transform_binding_args([ {<<"x-?ge ", K/binary>>, _T, V} | R ], Result, BT, LNXK) ->
    transform_binding_args (R, [ {K, ge, V} | Result], BT, LNXK);
transform_binding_args([ {<<"x-?lt ", K/binary>>, _T, V} | R ], Result, BT, LNXK) ->
    transform_binding_args (R, [ {K, lt, V} | Result], BT, LNXK);
transform_binding_args([ {<<"x-?le ", K/binary>>, _T, V} | R ], Result, BT, LNXK) ->
    transform_binding_args (R, [ {K, le, V} | Result], BT, LNXK);
transform_binding_args([ {<<"x-?eq ", K/binary>>, _T, V} | R ], Result, BT, LNXK) ->
    transform_binding_args (R, [ {K, eq, V} | Result], BT, LNXK);
transform_binding_args([ {<<"x-?ne ", K/binary>>, _T, V} | R ], Result, BT, LNXK) ->
    transform_binding_args (R, [ {K, ne, V} | Result], BT, LNXK);
transform_binding_args([{<<"x-match">>, longstr, <<"any">>} | R], Result, _, LNXK) ->
    transform_binding_args (R, Result, any, LNXK);
transform_binding_args([{<<"x-match">>, longstr, <<"all">>} | R], Result, _, LNXK) ->
    transform_binding_args (R, Result, all, LNXK);
transform_binding_args([ {<<"x-", _/binary>>, _T, _V} | R ], Result, BT, LNXK) ->
    transform_binding_args (R, Result, BT, LNXK);
transform_binding_args([ {K, _T, V} | R ], Result, BT, LNXK) ->
    transform_binding_args (R, [ {K, eq, V} | Result], BT, LNXK).



add_binding(transaction, X, BindingToAdd = #binding{destination = Dest, args = Args}) ->
    BindingId = crypto:hash(md5,term_to_binary(BindingToAdd)),
io:format ("AA", []),
    { CleanArgs, BindingType, LNXK } = transform_binding_args (Args),
io:format ("CA ~p~nBT ~p~nNX ~p~n", [CleanArgs, BindingType, LNXK]),
    NewR = #headers_bindings_keys{exchange = X, binding_id = BindingId},
io:format ("m ", []),
    mnesia:write (rabbit_headers_bindings_keys, NewR, write),
io:format ("k ", []),
    XR = #headers_bindings{exch_bind = {X, BindingId}, destination = Dest, binding_type = BindingType, last_nxkey = LNXK, cargs = rabbit_misc:sort_field_table(CleanArgs)},
io:format ("j : ~p~n", [XR]),
    mnesia:write (rabbit_headers_bindings, XR, write),
io:format ("i ", []);
add_binding(_Tx, _X, _B) -> ok.

%% Bs is a list here
remove_bindings(transaction, X, Bs) ->
    BindingsIDs_todel = [ crypto:hash(md5,term_to_binary(Binding)) || Binding <- Bs ],

    lists:foreach (fun(BindingID_todel) -> mnesia:delete ({ rabbit_headers_bindings, { X, BindingID_todel } }) end, BindingsIDs_todel),
    lists:foreach (
        fun(BindingID_todel) ->
            R_todel = #headers_bindings_keys{exchange = X, binding_id = BindingID_todel},
            mnesia:delete_object (rabbit_headers_bindings_keys, R_todel, write)
        end, BindingsIDs_todel);
remove_bindings(_Tx, _X, _Bs) -> ok.


validate(_X) -> ok.
create(_Tx, _X) -> ok.
delete(_Tx, _X, _Bs) -> ok.
policy_changed(_X1, _X2) -> ok.
assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).
