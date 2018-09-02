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

-module(rabbit_exchange_type_open).
-include_lib("stdlib/include/qlc.hrl").
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/3, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([info/1, info/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type open"},
                    {mfa,         {rabbit_registry, register,
                                   [exchange, <<"x-open">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

-define(ONE_CHAR_AT_LEAST, _/utf8, _/binary).

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{description, <<"Rabbit extension : AMQP open exchange, route like you want">>}].

serialise_events() -> false.

route(#exchange{name = Name},
      #delivery{message = #basic_message{content = Content}}) ->
    Headers = case (Content#content.properties)#'P_basic'.headers of
                  undefined -> [];
                  H         -> rabbit_misc:sort_field_table(H)
              end,
    CurrentBindings = case ets:lookup(rabbit_open_bindings, Name) of
        [] -> [];
        [#open_bindings{bindings = E}] -> E
    end,
    get_routes(Headers, CurrentBindings, ordsets:new()).

get_routes(_, [], ResDests) -> ordsets:to_list(ResDests);
get_routes(Headers, [ {BindingType, DAT, DAF, Args, _} | T ], ResDests) ->
    case headers_match(BindingType, Args, Headers) of
        true  -> get_routes(Headers, T, ordsets:union(DAT, ResDests));
        _ -> get_routes(Headers, T, ordsets:union(DAF, ResDests))
    end.

headers_match(all, Args, Headers) ->
    case headers_match_all_hkreex(Args, Headers) of
        true ->
            case headers_match_all_hkrenx(Args, Headers) of
                true -> headers_match_all(Args, Headers);
                _ -> false
            end;
        _ -> false
    end;
headers_match(any, Args, Headers) ->
    headers_match_any(Args, Headers).


validate_binding(_X, #binding{args = Args, key = << >>}) ->
    case rabbit_misc:table_lookup(Args, <<"x-match">>) of
        {longstr, <<"all">>} -> validate_list_type_usage(all, Args);
        {longstr, <<"any">>} -> validate_list_type_usage(any, Args);
        {longstr, Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field value ~p; "
                                  "expected all or any", [Other]}};
        {Type,    Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field type ~p (value ~p); "
                                  "expected longstr", [Type, Other]}};
        undefined            -> validate_list_type_usage(all, Args)
    end;
validate_binding(_X, _) ->
    {error, {binding_invalid, "Invalid routing key declaration in x-open exchange", []}}.


%% Binding's header keys of type 'list' must be validated
validate_list_type_usage(BindingType, Args) ->
    validate_list_type_usage(BindingType, Args, Args).

validate_list_type_usage(_, [], Args) -> validate_no_deep_lists(Args);
validate_list_type_usage(all, [ {<<"x-hkv?= ", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with = operator with binding type 'all'", []}};
validate_list_type_usage(any, [ {<<"x-hkv?!= ", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with != operator with binding type 'any'", []}};
validate_list_type_usage(any, [ {<<"x-hk?nx">>, _, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of nx operator with binding type 'any'", []}};
validate_list_type_usage(any, [ {<<"x-hkre?nx">>, _, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of re nx operator with binding type 'any'", []}};
validate_list_type_usage(BindingType, [ {<< RuleKey/binary >>, array, _} | Tail ], Args) ->
    RKL = binary_to_list(RuleKey),
    MatchOperators = ["x-hkv?<", "x-hkv?>", "x-hk?<", "x-hk?>", "x-hkvre?=", "x-hkvre?!="],
    case lists:filter(fun(S) -> lists:prefix(S, RKL) end, MatchOperators) of
        [] -> validate_list_type_usage(BindingType, Tail, Args);
        _ -> {error, {binding_invalid, "Invalid use of list type with comparison or regex operators", []}}
    end;
validate_list_type_usage(BindingType, [ _ | Tail ], Args) ->
    validate_list_type_usage(BindingType, Tail, Args).


%% Binding can't have array in array :
validate_no_deep_lists(Args) -> 
    validate_no_deep_lists(Args, [], Args).

validate_no_deep_lists([], [], Args) -> validate_operators(Args);
validate_no_deep_lists(_, [_ | _], _) ->
    {error, {binding_invalid, "Invalid use of list in list", []}};
validate_no_deep_lists([ {_, array, Vs} | Tail ], _, Args) ->
    ArrInArr = [ bad || {array, _} <- Vs ],
    validate_no_deep_lists(Tail, ArrInArr, Args); 
validate_no_deep_lists([ _ | Tail ], _, Args) ->
    validate_no_deep_lists(Tail, [], Args).



%% Binding is INvalidated if some rule does not match anything,
%%  so that we can't have some "unexpected" results
validate_operators(Args) ->
    io:format("A:~p~n", [Args]),
  FlattenedArgs = flatten_binding_args(Args),
    io:format("F:~p~n", [FlattenedArgs]),
  case validate_operators2(FlattenedArgs) of
    ok -> validate_regexes(Args);
    Err -> Err
  end.

validate_operators2([]) -> ok;
validate_operators2([ {_, decimal, _} | _ ]) ->
    {error, {binding_invalid, "Decimal type is invalid in x-open", []}};
validate_operators2([ {<<"x-match">>, longstr, _} | Tail ]) -> validate_operators2(Tail);
% Do not think that can't happen... it can ! :)
validate_operators2([ {<<>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Binding's rule key can't be void", []}};
%validate_operators2([ { K, T, V } | Tail ]) ->
%    io:format("K:~p T:~p V:~p~n", [K, T, V]),
%    validate_operators2(Tail);
validate_operators2([ {<<"x-hk?ex">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-hk?nx">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-match-addq-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-match-addq-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-match-adde-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-match-adde-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);

% Operators hkv with < or > must be numeric only.
validate_operators2([ {<<"x-hkv?<= ", ?ONE_CHAR_AT_LEAST>>, _, V} | Tail ]) when is_number(V) -> validate_operators2(Tail);
validate_operators2([ {<<"x-hkv?<= ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_operators2([ {<<"x-hkv?< ", ?ONE_CHAR_AT_LEAST>>, _, V} | Tail ]) when is_number(V) -> validate_operators2(Tail);
validate_operators2([ {<<"x-hkv?< ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_operators2([ {<<"x-hkv?>= ", ?ONE_CHAR_AT_LEAST>>, _, V} | Tail ]) when is_number(V) -> validate_operators2(Tail);
validate_operators2([ {<<"x-hkv?>= ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_operators2([ {<<"x-hkv?> ", ?ONE_CHAR_AT_LEAST>>, _, V} | Tail ]) when is_number(V) -> validate_operators2(Tail);
validate_operators2([ {<<"x-hkv?> ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};

validate_operators2([ {<<"x-hkv?= ", ?ONE_CHAR_AT_LEAST>>, _, _} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-hkvre?= ", ?ONE_CHAR_AT_LEAST>>, longstr, _} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-hkv?!= ", ?ONE_CHAR_AT_LEAST>>, _, _} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-hkvre?!= ", ?ONE_CHAR_AT_LEAST>>, longstr, _} | Tail ]) -> validate_operators2(Tail);

% RE on HK
validate_operators2([ {<<"x-hkre?ex">>, longstr, _} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-hkre?nx">>, longstr, _} | Tail ]) -> validate_operators2(Tail);

validate_operators2([ {InvalidKey = <<"x-", _/binary>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Binding's key ~p cannot start with 'x-' in x-open exchange; use new operators to match such keys", [InvalidKey]}};
validate_operators2([ _ | Tail ]) -> validate_operators2(Tail).


%validate_all_keys_are_printable([]) -> ok;
%validate_all_keys_are_printable([ {K, _, _} | Tail ]) ->
%    case io_lib:printable_unicode_list(unicode:characters_to_list(V)) of
%        true -> io:format("true~n", []), validate_operators2(Tail);
%        _ -> io:format("false~n", []), {error, {binding_invalid, "Type's value of comparison's operators must be string or numeric", []}}
%    end;
%validate_operators2([ {<<"x-hkv?<= ", ?ONE_CHAR_AT_LEAST>>, _, V} | Tail ]) when is_number(V) -> validate_operators2(Tail);
%validate_operators2([ {<<"x-hkv?<= ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
%    {error, {binding_invalid, "Type's value of comparison's operators must be string or numeric", []}};



%validate_regexes([]) -> ok;
%validate_regexes([ {<<"x-hkvre?=", _/binary>>, _, << RegexBin/binary >>} | Tail ]) ->
%    case re:compile(RegexBin) of
%        {ok, _} -> validate_regexes(Tail);
%        {error, _} -> {error, {binding_invalid, "Regex '~ts' is invalid", [RegexBin]}}
%    end;
%validate_regexes([ {<<"x-hkvre?!=", _/binary>>, _, << RegexBin/binary >>} | Tail ]) ->
%    case re:compile(Regex) of
%        {ok, _} -> validate_regexes(Tail);
%        {error, _} -> {error, {binding_invalid, "Regex '~ts' is invalid", [RegexBin]}}
%    end;
%validate_regexes([ _ | Tail ]) -> validate_regexes(Tail).


validate_regexes_item(RegexBin, Tail) ->
    case re:compile(RegexBin) of
        {ok, _} -> validate_regexes(Tail);
        _ -> {error, {binding_invalid, "Regex '~ts' is invalid", [RegexBin]}}
    end.

validate_regexes([]) -> ok;
validate_regexes([ {<< RuleKey:9/binary, _/binary >>, longstr, << RegexBin/binary >>} | Tail ]) when RuleKey==<<"x-hkvre?=">>; RuleKey==<<"x-hkre?ex">>; RuleKey==<<"x-hkre?nx">> ->
    validate_regexes_item(RegexBin, Tail);
validate_regexes([ {<< RuleKey:10/binary, _/binary >>, longstr, << RegexBin/binary >>} | Tail ]) when RuleKey==<<"x-hkvre?!=">> ->
    validate_regexes_item(RegexBin, Tail);
validate_regexes([ _ | Tail ]) ->
        validate_regexes(Tail).


%% [0] spec is vague on whether it can be omitted but in practice it's
%% useful to allow people to do this.
%% So, by default the binding type is 'all'; and that's it ! :)
parse_x_match({longstr, <<"all">>}) -> all;
parse_x_match({longstr, <<"any">>}) -> any;
parse_x_match(_)                    -> all.

%%
%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%% REQUIRES BOTH PATTERN AND DATA TO BE SORTED ASCENDING BY KEY.
%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%%

%% Binding type 'all' match

% Special funs to checks for keys (non-)existence via regex
headers_match_all_hkreex([], _) -> true;
headers_match_all_hkreex(BCur = [{_, reex, BV} | _], [{HK, _, _} | HNext]) ->
    case re:run(HK, BV, [ {capture, none} ]) of
        match -> headers_match_all_hkreex(BCur, HNext);
        _ -> false
    end;
headers_match_all_hkreex([_ | BNext], HCur) ->
    headers_match_all_hkreex(BNext, HCur).

headers_match_all_hkrenx([], _) -> true;
headers_match_all_hkrenx(BCur = [{_, renx, BV} | _], [{HK, _, _} | HNext]) ->
    case re:run(HK, BV, [ {capture, none} ]) of
        match -> false;
        _ -> headers_match_all_hkrenx(BCur, HNext)
    end;
headers_match_all_hkrenx([_ | BNext], HCur) ->
    headers_match_all_hkrenx(BNext, HCur).


% No more match operator to check; return true
headers_match_all([], _) -> true;

% Purge nx op on no data as all these are true
headers_match_all([{_, nx, _} | BNext], []) ->
    headers_match_all(BNext, []);

% Purge reex and renx ops as they have benn checked upstream
headers_match_all([{_, reex, _} | BNext], HCur) ->
    headers_match_all(BNext, HCur);
headers_match_all([{_, renx, _} | BNext], HCur) ->
    headers_match_all(BNext, HCur);

% No more message header but still match operator to check; return false
headers_match_all(_, []) -> false;

% Current header key not in match operators; go next header with current match operator
headers_match_all(BCur = [{BK, _, _} | _], [{HK, _, _} | HNext])
    when BK > HK -> headers_match_all(BCur, HNext);
% Current binding key must not exist in data, go next binding
headers_match_all([{BK, nx, _} | BNext], HCur = [{HK, _, _} | _])
    when BK < HK -> headers_match_all(BNext, HCur);
% Current match operator does not exist in message; return false
headers_match_all([{BK, _, _} | _], [{HK, _, _} | _])
    when BK < HK -> false;
%
% From here, BK == HK (keys are the same)
%
% Current values must match and do match; ok go next
headers_match_all([{_, eq, BV} | BNext], [{_, _, HV} | HNext])
    when BV == HV -> headers_match_all(BNext, HNext);
% Current values must match but do not match; return false
headers_match_all([{_, eq, _} | _], _) -> false;
% Key must not exist, return false
headers_match_all([{_, nx, _} | _], _) -> false;
% Current header key must exist; ok go next
headers_match_all([{_, ex, _} | BNext], [ _ | HNext]) ->
    headers_match_all(BNext, HNext);
% <= < = != > >=
headers_match_all([{_, ne, BV} | BNext], HCur = [{_, _, HV} | _])
    when BV /= HV -> headers_match_all(BNext, HCur);
headers_match_all([{_, ne, _} | _], _) -> false;

% Thanks to validation done upstream, gt/ge/lt/le are done only for numeric
headers_match_all([{_, gt, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV > BV -> headers_match_all(BNext, HCur);
headers_match_all([{_, gt, _} | _], _) -> false;
headers_match_all([{_, ge, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV >= BV -> headers_match_all(BNext, HCur);
headers_match_all([{_, ge, _} | _], _) -> false;
headers_match_all([{_, lt, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV < BV -> headers_match_all(BNext, HCur);
headers_match_all([{_, lt, _} | _], _) -> false;
headers_match_all([{_, le, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV =< BV -> headers_match_all(BNext, HCur);
headers_match_all([{_, le, _} | _], _) -> false;

% Regexes
headers_match_all([{_, re, BV} | BNext], [{_, longstr, HV} | HNext]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> headers_match_all(BNext, HNext);
        _ -> false
    end;
% Message header value is not a string : regex returns always false :
headers_match_all([{_, re, _} | _], _) -> false;
headers_match_all([{_, nre, BV} | BNext], [{_, longstr, HV} | HNext]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        nomatch -> headers_match_all(BNext, HNext);
        _ -> false
    end;
% Message header value is not a string : regex returns always false :
headers_match_all([{_, nre, _} | _], _) -> false.



%% Binding type 'any' match

% No more match operator to check; return false
headers_match_any([], _) -> false;
% No more message header but still match operator to check; return false
headers_match_any(_, []) -> false;
% Current header key not in match operators; go next header with current match operator
headers_match_any(BCur = [{BK, _, _} | _], [{HK, _, _} | HNext])
    when BK > HK -> headers_match_any(BCur, HNext);
% Current binding key does not exist in message; go next binding
headers_match_any([{BK, _, _} | BNext], HCur = [{HK, _, _} | _])
    when BK < HK -> headers_match_any(BNext, HCur);
%
% From here, BK == HK
%
% Current values must match and do match; return true
headers_match_any([{_, eq, BV} | _], [{_, _, HV} | _]) when BV == HV -> true;
% Current header key must exist; return true
headers_match_any([{_, ex, _} | _], _) -> true;
headers_match_any([{_, ne, BV} | _], [{_, _, HV} | _]) when HV /= BV -> true;
headers_match_any([{_, gt, BV} | _], [{_, _, HV} | _]) when HV > BV -> true;
headers_match_any([{_, ge, BV} | _], [{_, _, HV} | _]) when HV >= BV -> true;
headers_match_any([{_, lt, BV} | _], [{_, _, HV} | _]) when HV < BV -> true;
headers_match_any([{_, le, BV} | _], [{_, _, HV} | _]) when HV =< BV -> true;

% Regexes
headers_match_any([{_, re, BV} | BNext], HCur = [ {_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> true;
        _ -> headers_match_any(BNext, HCur)
    end;
headers_match_any([{_, nre, BV} | BNext], HCur = [ {_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> headers_match_any(BNext, HCur);
        _ -> true
    end;
% No match yet; go next
headers_match_any([_ | BNext], HCur) ->
    headers_match_any(BNext, HCur).


get_match_operators(BindingArgs) ->
    MatchOperators = get_match_operators(BindingArgs, []),
    rabbit_misc:sort_field_table(MatchOperators).

% We won't check types again as this has been done during validation..
get_match_operators([], Result) -> Result;
% Does a key exist ?
get_match_operators([ {<<"x-hk?ex">>, _, K} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, ex, nil} | Res]);
% Does a key NOT exist ?
get_match_operators([ {<<"x-hk?nx">>, _, K} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, nx, nil} | Res]);
% Does a key match a regex ?
get_match_operators([ {<<"x-hkre?ex">>, _, K} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, reex, K} | Res]);
% Does a key NOT match a regex ?
get_match_operators([ {<<"x-hkre?nx">>, _, K} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, renx, K} | Res]);

% operators <= < = != > >=
get_match_operators([ {<<"x-hkv?<= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, le, V} | Res]);
get_match_operators([ {<<"x-hkv?< ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, lt, V} | Res]);
get_match_operators([ {<<"x-hkv?= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, eq, V} | Res]);
get_match_operators([ {<<"x-hkvre?= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, re, binary_to_list(V)} | Res]);
get_match_operators([ {<<"x-hkv?!= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, ne, V} | Res]);
get_match_operators([ {<<"x-hkvre?!= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, nre, binary_to_list(V)} | Res]);
get_match_operators([ {<<"x-hkv?> ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, gt, V} | Res]);
get_match_operators([ {<<"x-hkv?>= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_operators (Tail, [ {K, ge, V} | Res]);
% for all other cases, the match operator is 'eq'
get_match_operators([ {K, _, V} | T ], Res) ->
    get_match_operators (T, [ {K, eq, V} | Res]).


%% DAT : Destinations to Add on True
%% DAF : Destinations to Add on False
get_dests_operators(VHost, Args) ->
    get_dests_operators(VHost, Args, ordsets:new(), ordsets:new()).

get_dests_operators(_, [], DAT,DAF) -> {DAT,DAF};
get_dests_operators(VHost, [{<<"x-match-addq-ontrue">>, longstr, D} | T], DAT,DAF) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, ordsets:add_element(R,DAT), DAF);
get_dests_operators(VHost, [{<<"x-match-adde-ontrue">>, longstr, D} | T], DAT,DAF) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, ordsets:add_element(R,DAT), DAF);
get_dests_operators(VHost, [{<<"x-match-addq-onfalse">>, longstr, D} | T], DAT,DAF) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, DAT, ordsets:add_element(R,DAF));
get_dests_operators(VHost, [{<<"x-match-adde-onfalse">>, longstr, D} | T], DAT,DAF) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, DAT, ordsets:add_element(R,DAF));
get_dests_operators(VHost, [_ | T], DAT,DAF) ->
    get_dests_operators(VHost, T, DAT,DAF).


%% Flatten one level for list of values (array)
flatten_binding_args(Args) ->
    flatten_binding_args(Args, []).

flatten_binding_args([], Result) -> Result;
flatten_binding_args ([ {K, array, Vs} | Tail ], Result) ->
        Res = [ { K, T, V } || {T, V} <- Vs ],
        flatten_binding_args (Tail, lists:append ([ Res , Result ]));
flatten_binding_args ([ {K, T, V} | Tail ], Result) ->
        flatten_binding_args (Tail, [ {K, T, V} | Result ]).


validate(_X) -> ok.
create(_Tx, _X) -> ok.

delete(transaction, #exchange{name = XName}, _) ->
    ok = mnesia:delete (rabbit_open_bindings, XName, write);
delete(_, _, _) -> ok.

policy_changed(_X1, _X2) -> ok.

add_binding(transaction, #exchange{name = #resource{virtual_host = VHost} = XName}, BindingToAdd = #binding{destination = Dest, args = BindingArgs}) ->
% BindingId is used to track original binding definition so that it is used when deleting later
    BindingId = crypto:hash(md5, term_to_binary(BindingToAdd)),
% Let's doing that heavy lookup one time only
    BindingType = parse_x_match(rabbit_misc:table_lookup(BindingArgs, <<"x-match">>)),
    FlattenedBindindArgs = flatten_binding_args(BindingArgs),
    MatchOperators = get_match_operators(FlattenedBindindArgs),
    {DAT, DAF} = get_dests_operators(VHost, FlattenedBindindArgs),
    CurrentBindings = case mnesia:read(rabbit_open_bindings, XName, write) of
        [] -> [];
        [#open_bindings{bindings = E}] -> E
    end,
    NewBinding = {BindingType, ordsets:add_element(Dest, DAT), DAF, MatchOperators, BindingId},
    NewBindings = [NewBinding | CurrentBindings],
    NewRecord = #open_bindings{exchange_name = XName, bindings = NewBindings},
    ok = mnesia:write(rabbit_open_bindings, NewRecord, write);
add_binding(_, _, _) ->
    ok.

remove_bindings(transaction, #exchange{name = XName}, BindingsToDelete) ->
    CurrentBindings = case mnesia:read(rabbit_open_bindings, XName, write) of
        [] -> [];
        [#open_bindings{bindings = E}] -> E
    end,
    BindingIdsToDelete = [crypto:hash(md5, term_to_binary(B)) || B <- BindingsToDelete],
    NewBindings = remove_bindings_ids(BindingIdsToDelete, CurrentBindings, []),
    NewRecord = #open_bindings{exchange_name = XName, bindings = NewBindings},
    ok = mnesia:write(rabbit_open_bindings, NewRecord, write);
remove_bindings(_, _, _) ->
    ok.

remove_bindings_ids(_, [], Res) -> Res;
remove_bindings_ids(BindingIdsToDelete, [Bind = {_,_,_,_,BId} | T], Res) ->
    case lists:member(BId, BindingIdsToDelete) of
        true -> remove_bindings_ids(BindingIdsToDelete, T, Res);
        _    -> remove_bindings_ids(BindingIdsToDelete, T, lists:append(Res, [Bind]))
    end.


assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

