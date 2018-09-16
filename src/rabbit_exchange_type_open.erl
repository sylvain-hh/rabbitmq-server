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
      #delivery{message = #basic_message{content = Content, routing_keys = [RK | _]}}) ->
    Headers = case (Content#content.properties)#'P_basic'.headers of
                  undefined -> [];
                  H         -> rabbit_misc:sort_field_table(H)
              end,
    CurrentOrderedBindings = case ets:lookup(rabbit_open_bindings, Name) of
        [] -> [];
        [#open_bindings{bindings = Bs}] -> Bs
    end,
    get_routes({RK, Headers}, CurrentOrderedBindings, 0, ordsets:new()).


is_match(BindingType, MatchRk, RK, Args, Headers) ->
    case BindingType of
        all -> is_match_rk(BindingType, MatchRk, RK) andalso is_match_hkv(BindingType, Args, Headers);
        any -> is_match_rk(BindingType, MatchRk, RK) orelse is_match_hkv(BindingType, Args, Headers)
    end.

get_routes(_, [], _, ResDests) -> ordsets:to_list(ResDests);
get_routes(Data={RK, Headers}, [ {_, BindingType, Dest, {Args, MatchRk}, _} | T ], _, ResDests) ->
    case is_match(BindingType, MatchRk, RK, Args, Headers) of
        true -> get_routes(Data, T, 0, ordsets:add_element(Dest, ResDests));
           _ -> get_routes(Data, T, 0, ResDests)
    end;
% Jump to the next binding satisfying the last goto operator
get_routes(Data, [ {Order, _, _, _, _, _} | T ], GotoOrder, ResDests) when GotoOrder > Order ->
    get_routes(Data, T, GotoOrder, ResDests);
get_routes(Data={RK, Headers}, [ {_, BindingType, Dest, {Args, MatchRk}, {GOT, GOF, StopOperators}, _} | T ], _, ResDests) ->
    case {is_match(BindingType, MatchRk, RK, Args, Headers), StopOperators} of
        {true,{1,_}}  -> ordsets:add_element(Dest, ResDests);
        {false,{_,1}} -> ResDests;
        {true,_}      -> get_routes(Data, T, GOT, ordsets:add_element(Dest, ResDests));
        {false,_}     -> get_routes(Data, T, GOF, ResDests)
    end;
get_routes(Data={RK, Headers}, [ {_, BindingType, Dest, {Args, MatchRk}, {GOT, GOF, StopOperators, DAT, DAF, DDT, DDF, nil, nil, nil, nil, nil, nil, nil, nil}, _} | T ], _, ResDests) ->
    case {is_match(BindingType, MatchRk, RK, Args, Headers), StopOperators} of
        {true,{1,_}}  -> ordsets:subtract(ordsets:union(DAT, ordsets:add_element(Dest, ResDests)), DDT);
        {false,{_,1}} -> ordsets:subtract(ordsets:union(DAF, ResDests), DDF);
        {true,_}      -> get_routes(Data, T, GOT, ordsets:subtract(ordsets:union(DAT, ordsets:add_element(Dest, ResDests)), DDT));
        {false,_}     -> get_routes(Data, T, GOF, ordsets:subtract(ordsets:union(DAF, ResDests), DDF))
    end;
get_routes(Data={RK, Headers}, [ {_, BindingType, Dest, {Args, MatchRk}, {GOT, GOF, StopOperators, DAT, DAF, DDT, DDF, VHost, DATRE, DAFRE, DDTRE, DDFRE, DATNRE, DAFNRE, DDTNRE, DDFNRE}, _} | T ], _, ResDests) ->
% May I use ets:tab2list here ?..... I don't know.
% And yes, maybe there is a cleaner way to list queues :)
%
%
% WE MUST DO THAT ONCE PER MESSAGE..
%
%
    AllQueues = mnesia:dirty_all_keys(rabbit_queue),
% We should drop amq.* queues also no ?! I don't see them here..
    AllVHQueues = [Q || Q = #resource{virtual_host = QueueVHost, kind = queue} <- AllQueues, QueueVHost == VHost],
    DATREsult = case DATRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DATRE, [ {capture, none} ]) == match])
    end,
    DAFREsult = case DAFRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DAFRE, [ {capture, none} ]) == match])
    end,
    DDTREsult = case DDTRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DDTRE, [ {capture, none} ]) == match])
    end,
    DDFREsult = case DDFRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DDFRE, [ {capture, none} ]) == match])
    end,
    DATNREsult = case DATNRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DATNRE, [ {capture, none} ]) /= match])
    end,
    DAFNREsult = case DAFNRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DAFNRE, [ {capture, none} ]) /= match])
    end,
    DDTNREsult = case DDTNRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DDTNRE, [ {capture, none} ]) /= match])
    end,
    DDFNREsult = case DDFNRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DDFNRE, [ {capture, none} ]) /= match])
    end,
    case {is_match(BindingType, MatchRk, RK, Args, Headers), StopOperators} of
        {true,{1,_}}  -> ordsets:subtract(ordsets:add_element(Dest, ordsets:union([DAT,DATREsult,DATNREsult,ResDests])), ordsets:union([DDT,DDTREsult,DDTNREsult]));
        {false,{_,1}} -> ordsets:subtract(ordsets:union([DAF,DAFREsult,DAFNREsult,ResDests]), ordsets:union([DDF,DDFREsult,DDFNREsult]));
        {true,_}      -> get_routes(Data, T, GOT, ordsets:subtract(ordsets:add_element(Dest, ordsets:union([DAT,DATREsult,DATNREsult,ResDests])), ordsets:union([DDT,DDTREsult,DDTNREsult])));
        {false,_}     -> get_routes(Data, T, GOF, ordsets:subtract(ordsets:union([DAF,DAFREsult,DAFNREsult,ResDests]), ordsets:union([DDF,DDFREsult,DDFNREsult])))
    end.


is_match_hkv(all, Args, Headers) ->
    is_match_hkv_all(Args, Headers);
is_match_hkv(any, Args, Headers) ->
    is_match_hkv_any(Args, Headers).


validate_binding(_X, #binding{args = Args, key = << >>, destination = Dest}) ->
    Args2 = rebuild_args(Args, Dest),
    case rabbit_misc:table_lookup(Args2, <<"x-match">>) of
        {longstr, <<"all">>} -> validate_list_type_usage(all, Args2);
        {longstr, <<"any">>} -> validate_list_type_usage(any, Args2);
        {longstr, Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field value ~p; "
                                  "expected all or any", [Other]}};
        {Type,    Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field type ~p (value ~p); "
                                  "expected longstr", [Type, Other]}};
        undefined            -> validate_list_type_usage(all, Args2)
    end;
validate_binding(_X, _) ->
    {error, {binding_invalid, "Invalid routing key declaration in x-open exchange", []}}.


%% Binding's header keys of type 'list' must be validated
validate_list_type_usage(BindingType, Args) ->
    validate_list_type_usage(BindingType, Args, Args).

validate_list_type_usage(_, [], Args) -> validate_no_deep_lists(Args);
% Routing key ops
validate_list_type_usage(all, [ {<<"x-?rk=", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with routing key = operator with binding type 'all'", []}};
validate_list_type_usage(any, [ {<<"x-?rk!=", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with routing key != operator with binding type 'any'", []}};
 
validate_list_type_usage(all, [ {<<"x-?hkv= ", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with = operator with binding type 'all'", []}};
validate_list_type_usage(any, [ {<<"x-?hkv!= ", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with != operator with binding type 'any'", []}};
validate_list_type_usage(BindingType, [ {<< RuleKey/binary >>, array, _} | Tail ], Args) ->
    RKL = binary_to_list(RuleKey),
    MatchOperators = ["x-?hkv<", "x-?hkv>"],
    case lists:filter(fun(S) -> lists:prefix(S, RKL) end, MatchOperators) of
        [] -> validate_list_type_usage(BindingType, Tail, Args);
        _ -> {error, {binding_invalid, "Invalid use of list type with < or > operators", []}}
    end;
% Else go next
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
  FlattenedArgs = flatten_binding_args(Args),
  case validate_operators2(FlattenedArgs) of
    ok -> validate_regexes(Args);
    Err -> Err
  end.

validate_operators2([]) -> ok;
% x-match have been checked first, so that we don't check type or value
validate_operators2([ {<<"x-match">>, _, _} | Tail ]) -> validate_operators2(Tail);
% Decimal type is a pain to compare with other numeric types's values, so this is forbidden.
validate_operators2([ {_, decimal, _} | _ ]) ->
    {error, {binding_invalid, "Decimal type is invalid in x-open", []}};
% Do not think that can't happen... it can ! :)
validate_operators2([ {<<>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Binding's rule key can't be void", []}};

% Routing key ops
validate_operators2([ {<<"x-?rk=">>, longstr, <<_/binary>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?rk!=">>, longstr, <<_/binary>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?rkre">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?rk!re">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);

validate_operators2([ {<<"x-?hkex">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hknx">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);

% Dests ops (exchanges)
validate_operators2([ {<<"x-adde-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-adde-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-dele-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-dele-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
% Dests ops (queues)
validate_operators2([ {<<"x-addq-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-addqre-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-addq!re-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-addq-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-addqre-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-addq!re-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-delq-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-delqre-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-delq!re-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-delq-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-delqre-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-delq!re-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);

% Binding order is numeric only
validate_operators2([ {<<"x-order">>, _, V} | Tail ]) when is_integer(V) -> validate_operators2(Tail);

% Gotos are numeric only
validate_operators2([ {<<"x-goto-ontrue">>, _, V} | Tail ]) when is_integer(V) -> validate_operators2(Tail);
validate_operators2([ {<<"x-goto-onfalse">>, _, V} | Tail ]) when is_integer(V) -> validate_operators2(Tail);

% Stops
validate_operators2([ {<<"x-stop-ontrue">>, longstr, <<>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-stop-onfalse">>, longstr, <<>>} | Tail ]) -> validate_operators2(Tail);

% Operators hkv with < or > must be numeric only.
validate_operators2([ {<<"x-?hkv<= ", ?ONE_CHAR_AT_LEAST>>, _, V} | Tail ]) when is_number(V) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hkv<= ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_operators2([ {<<"x-?hkv< ", ?ONE_CHAR_AT_LEAST>>, _, V} | Tail ]) when is_number(V) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hkv< ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_operators2([ {<<"x-?hkv>= ", ?ONE_CHAR_AT_LEAST>>, _, V} | Tail ]) when is_number(V) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hkv>= ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_operators2([ {<<"x-?hkv> ", ?ONE_CHAR_AT_LEAST>>, _, V} | Tail ]) when is_number(V) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hkv> ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};

validate_operators2([ {<<"x-?hkv= ", ?ONE_CHAR_AT_LEAST>>, _, _} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hkvre ", ?ONE_CHAR_AT_LEAST>>, longstr, _} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hkv!= ", ?ONE_CHAR_AT_LEAST>>, _, _} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hkv!re ", ?ONE_CHAR_AT_LEAST>>, longstr, _} | Tail ]) -> validate_operators2(Tail);

validate_operators2([ {InvalidKey = <<"x-", _/binary>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Binding's key ~p cannot start with 'x-' in x-open exchange; use new operators to match such keys", [InvalidKey]}};
validate_operators2([ _ | Tail ]) -> validate_operators2(Tail).


validate_regexes_item(RegexBin, Tail) ->
    case re:compile(RegexBin) of
        {ok, _} -> validate_regexes(Tail);
        _ -> {error, {binding_invalid, "Regex '~ts' is invalid", [RegexBin]}}
    end.

validate_regexes([]) -> ok;
validate_regexes([ {<< RuleKey:8/binary, _/binary >>, longstr, << RegexBin/binary >>} | Tail ]) when RuleKey==<<"x-?hkvre">> ; RuleKey==<<"x-addqre">> ; RuleKey==<<"x-delqre">> ->
    validate_regexes_item(RegexBin, Tail);
validate_regexes([ {<< RuleKey:9/binary, _/binary >>, longstr, << RegexBin/binary >>} | Tail ]) when RuleKey==<<"x-?hkv!re">> ; RuleKey==<<"x-addq!re">> ; RuleKey==<<"x-delq!re">> ->
    validate_regexes_item(RegexBin, Tail);
validate_regexes([ _ | Tail ]) ->
        validate_regexes(Tail).


%% [0] spec is vague on whether it can be omitted but in practice it's
%% useful to allow people to do this.
%% So, by default the binding type is 'all'; and that's it ! :)
parse_x_match({longstr, <<"all">>}) -> all;
parse_x_match({longstr, <<"any">>}) -> any;
parse_x_match(_)                    -> all.


%
% Check RK operators with current routing key from message
%

is_match_rk(all, Rules, RK) ->
    is_match_rk_all(Rules, RK);
is_match_rk(any, Rules, RK) ->
    is_match_rk_any(Rules, RK).

% With 'all' binding type
% No (more) match to chek, return true
is_match_rk_all([], _) -> true;
% Case RK must be equal
is_match_rk_all([ {rkeq, V} | Tail], RK) when V == RK ->
    is_match_rk_all(Tail, RK);
% Case RK must not be equal
is_match_rk_all([ {rkne, V} | Tail], RK) when V /= RK ->
    is_match_rk_all(Tail, RK);
% Case RK must match regex
is_match_rk_all([ {rkre, V} | Tail], RK) ->
    case re:run(RK, V, [ {capture, none} ]) of
        match -> is_match_rk_all(Tail, RK);
        _ -> false
    end;
% Case RK must not match regex
is_match_rk_all([ {rknre, V} | Tail], RK) ->
    case re:run(RK, V, [ {capture, none} ]) of
        match -> false;
        _ -> is_match_rk_all(Tail, RK)
    end;
% rkeq or rkne are false..
is_match_rk_all(_, _) ->
    false.

% With 'any' binding type
% No (more) match to chek, return false
is_match_rk_any([], _) -> false;
% Case RK must be equal
is_match_rk_any([ {rkeq, V} | _], RK) when V == RK -> true;
% Case RK must not be equal
is_match_rk_any([ {rkne, V} | _], RK) when V /= RK -> true;
% Case RK must match regex
is_match_rk_any([ {rkre, V} | Tail], RK) ->
    case re:run(RK, V, [ {capture, none} ]) of
        match -> true;
        _ -> is_match_rk_any(Tail, RK)
    end;
% Case RK must not match regex
is_match_rk_any([ {rknre, V} | Tail], RK) ->
    case re:run(RK, V, [ {capture, none} ]) of
        match -> is_match_rk_any(Tail, RK);
        _ -> true
    end;
% rkeq or rkne are false..
is_match_rk_any([ _ | Tail], RK) ->
    is_match_rk_any(Tail, RK).






%%
%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%% REQUIRES BOTH PATTERN AND DATA TO BE SORTED ASCENDING BY KEY.
%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%%

%% Binding type 'all' match

% No more match operator to check; return true
is_match_hkv_all([], _) -> true;

% Purge nx op on no data as all these are true
is_match_hkv_all([{_, nx, _} | BNext], []) ->
    is_match_hkv_all(BNext, []);

% No more message header but still match operator to check; return false
is_match_hkv_all(_, []) -> false;

% Current header key not in match operators; go next header with current match operator
is_match_hkv_all(BCur = [{BK, _, _} | _], [{HK, _, _} | HNext])
    when BK > HK -> is_match_hkv_all(BCur, HNext);
% Current binding key must not exist in data, go next binding
is_match_hkv_all([{BK, nx, _} | BNext], HCur = [{HK, _, _} | _])
    when BK < HK -> is_match_hkv_all(BNext, HCur);
% Current match operator does not exist in message; return false
is_match_hkv_all([{BK, _, _} | _], [{HK, _, _} | _])
    when BK < HK -> false;
%
% From here, BK == HK (keys are the same)
%
% Current values must match and do match; ok go next
is_match_hkv_all([{_, eq, BV} | BNext], [{_, _, HV} | HNext])
    when BV == HV -> is_match_hkv_all(BNext, HNext);
% Current values must match but do not match; return false
is_match_hkv_all([{_, eq, _} | _], _) -> false;
% Key must not exist, return false
is_match_hkv_all([{_, nx, _} | _], _) -> false;
% Current header key must exist; ok go next
is_match_hkv_all([{_, ex, _} | BNext], [ _ | HNext]) ->
    is_match_hkv_all(BNext, HNext);
% <= < = != > >=
is_match_hkv_all([{_, ne, BV} | BNext], HCur = [{_, _, HV} | _])
    when BV /= HV -> is_match_hkv_all(BNext, HCur);
is_match_hkv_all([{_, ne, _} | _], _) -> false;

% Thanks to validation done upstream, gt/ge/lt/le are done only for numeric
is_match_hkv_all([{_, gt, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV > BV -> is_match_hkv_all(BNext, HCur);
is_match_hkv_all([{_, gt, _} | _], _) -> false;
is_match_hkv_all([{_, ge, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV >= BV -> is_match_hkv_all(BNext, HCur);
is_match_hkv_all([{_, ge, _} | _], _) -> false;
is_match_hkv_all([{_, lt, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV < BV -> is_match_hkv_all(BNext, HCur);
is_match_hkv_all([{_, lt, _} | _], _) -> false;
is_match_hkv_all([{_, le, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV =< BV -> is_match_hkv_all(BNext, HCur);
is_match_hkv_all([{_, le, _} | _], _) -> false;

% Regexes
is_match_hkv_all([{_, re, BV} | BNext], HCur = [{_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> is_match_hkv_all(BNext, HCur);
        _ -> false
    end;
% Message header value is not a string : regex returns always false :
is_match_hkv_all([{_, re, _} | _], _) -> false;
is_match_hkv_all([{_, nre, BV} | BNext], HCur = [{_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        nomatch -> is_match_hkv_all(BNext, HCur);
        _ -> false
    end;
% Message header value is not a string : regex returns always false :
is_match_hkv_all([{_, nre, _} | _], _) -> false.



%% Binding type 'any' match

% No more match operator to check; return false
is_match_hkv_any([], _) -> false;
% No more message header but still match operator to check; return false
is_match_hkv_any(_, []) -> false;
% Current header key not in match operators; go next header with current match operator
is_match_hkv_any(BCur = [{BK, _, _} | _], [{HK, _, _} | HNext])
    when BK > HK -> is_match_hkv_any(BCur, HNext);
% Current binding key must not exist in data, return true
is_match_hkv_any([{BK, nx, _} | _], [{HK, _, _} | _])
    when BK < HK -> true;
% Current binding key does not exist in message; go next binding
is_match_hkv_any([{BK, _, _} | BNext], HCur = [{HK, _, _} | _])
    when BK < HK -> is_match_hkv_any(BNext, HCur);
%
% From here, BK == HK
%
% Current values must match and do match; return true
is_match_hkv_any([{_, eq, BV} | _], [{_, _, HV} | _]) when BV == HV -> true;
% Current header key must exist; return true
is_match_hkv_any([{_, ex, _} | _], _) -> true;
is_match_hkv_any([{_, ne, BV} | _], [{_, _, HV} | _]) when HV /= BV -> true;
is_match_hkv_any([{_, gt, BV} | _], [{_, _, HV} | _]) when HV > BV -> true;
is_match_hkv_any([{_, ge, BV} | _], [{_, _, HV} | _]) when HV >= BV -> true;
is_match_hkv_any([{_, lt, BV} | _], [{_, _, HV} | _]) when HV < BV -> true;
is_match_hkv_any([{_, le, BV} | _], [{_, _, HV} | _]) when HV =< BV -> true;

% Regexes
is_match_hkv_any([{_, re, BV} | BNext], HCur = [ {_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> true;
        _ -> is_match_hkv_any(BNext, HCur)
    end;
is_match_hkv_any([{_, nre, BV} | BNext], HCur = [ {_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> is_match_hkv_any(BNext, HCur);
        _ -> true
    end;
% No match yet; go next
is_match_hkv_any([_ | BNext], HCur) ->
    is_match_hkv_any(BNext, HCur).


% This fun transforms some rule by some other
rebuild_args(Args, Dest) -> rebuild_args(Args, [], Dest).

rebuild_args([], Ret, _) -> Ret;
rebuild_args([ {<<"x-delq-main">>, longstr, <<>>} | Tail ], Ret, Dest = #resource{kind = queue, name = RName} ) ->
    rebuild_args(Tail, [ {<<"x-delq-ontrue">>, longstr, RName} | Ret], Dest);
rebuild_args([ Op | Tail ], Ret, Dest) ->
    rebuild_args(Tail, [ Op | Ret], Dest).



get_match_hk_ops(BindingArgs) ->
    MatchOperators = get_match_hk_ops(BindingArgs, []),
    rabbit_misc:sort_field_table(MatchOperators).

% Get match operators
% We won't check types again as this has been done during validation..

% Get match operators based on headers
get_match_hk_ops([], Result) -> Result;
% Does a key exist ?
get_match_hk_ops([ {<<"x-?hkex">>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {V, ex, nil} | Res]);
% Does a key NOT exist ?
get_match_hk_ops([ {<<"x-?hknx">>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {V, nx, nil} | Res]);

% operators <= < = != > >=
get_match_hk_ops([ {<<"x-?hkv<= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, le, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv< ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, lt, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, eq, V} | Res]);
get_match_hk_ops([ {<<"x-?hkvre ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, re, binary_to_list(V)} | Res]);
get_match_hk_ops([ {<<"x-?hkv!= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, ne, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv!re ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, nre, binary_to_list(V)} | Res]);
get_match_hk_ops([ {<<"x-?hkv> ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, gt, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv>= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, ge, V} | Res]);

%% We should not found here another header beginning with x-? !!!

%% All others beginnig with x- are other operators
get_match_hk_ops([ {<<"x-", _/binary>>, _, _} | Tail ], Res) ->
    get_match_hk_ops (Tail, Res);
% And for all other cases, the match operator is 'eq'
get_match_hk_ops([ {K, _, V} | T ], Res) ->
    get_match_hk_ops (T, [ {K, eq, V} | Res]).


% Get match operators related to routing key
get_match_rk_ops([], Result) -> Result;

get_match_rk_ops([ {<<"x-?rk=">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_rk_ops(Tail, [ {rkeq, V} | Res]);
get_match_rk_ops([ {<<"x-?rk!=">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_rk_ops(Tail, [ {rkne, V} | Res]);
get_match_rk_ops([ {<<"x-?rkre">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_rk_ops(Tail, [ {rkre, V} | Res]);
get_match_rk_ops([ {<<"x-?rk!re">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_rk_ops(Tail, [ {rknre, V} | Res]);
get_match_rk_ops([ _ | Tail ], Result) ->
    get_match_rk_ops(Tail, Result).


% Validation is made upstream
get_binding_order(Args, Default) ->
    case rabbit_misc:table_lookup(Args, <<"x-order">>) of
        undefined     -> Default;
        {_, Order} -> Order
    end.

% Validation is made upstream
get_stop_operators([], Result) -> Result;
get_stop_operators([{<<"x-stop-ontrue">>, _, _} | T], {_, StopOnFalse}) ->
    get_stop_operators(T, {1, StopOnFalse});
get_stop_operators([{<<"x-stop-onfalse">>, _, _} | T], {StopOnTrue, _}) ->
    get_stop_operators(T, {StopOnTrue, 1});
get_stop_operators([_ | T], R) ->
    get_stop_operators(T, R).

% Validation is made upstream
get_goto_operators([], Result) -> Result;
get_goto_operators([{<<"x-goto-ontrue">>, _, N} | T], {_, GotoOnFalse}) ->
    get_goto_operators(T, {N, GotoOnFalse});
get_goto_operators([{<<"x-goto-onfalse">>, _, N} | T], {GotoOnTrue, _}) ->
    get_goto_operators(T, {GotoOnTrue, N});
get_goto_operators([_ | T], R) ->
    get_goto_operators(T, R).


%% DAT : Destinations to Add on True
%% DAF : Destinations to Add on False
%% DDT : Destinations to Del on True
%% DDF : Destinations to Del on False
% They are resource's names or regex
get_dests_operators(VHost, Args) ->
    OS = ordsets:new(),
    get_dests_operators(VHost, Args, {OS, OS, OS, OS}, {nil, nil, nil, nil, nil, nil, nil, nil}).

get_dests_operators(_, [], Dests, DestsRE) -> {Dests, DestsRE};
get_dests_operators(VHost, [{<<"x-addq-ontrue">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, {ordsets:add_element(R,DAT), DAF, DDT, DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-adde-ontrue">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, {ordsets:add_element(R,DAT), DAF, DDT, DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-addq-onfalse">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, {DAT, ordsets:add_element(R,DAF), DDT, DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-adde-onfalse">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, {DAT, ordsets:add_element(R,DAF), DDT, DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-delq-ontrue">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, {DAT, DAF, ordsets:add_element(R,DDT), DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-dele-ontrue">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, {DAT, DAF, ordsets:add_element(R,DDT), DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-delq-onfalse">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, {DAT, DAF, DDT, ordsets:add_element(R,DDF)}, DestsRE);
get_dests_operators(VHost, [{<<"x-dele-onfalse">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, {DAT, DAF, DDT, ordsets:add_element(R,DDF)}, DestsRE);
% Regex part
get_dests_operators(VHost, [{<<"x-addqre-ontrue">>, longstr, R} | T], Dests, {_,DAFRE,DDTRE,DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {R, DAFRE, DDTRE, DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-addqre-onfalse">>, longstr, R} | T], Dests, {DATRE,_,DDTRE,DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, R, DDTRE, DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-delqre-ontrue">>, longstr, R} | T], Dests, {DATRE,DAFRE,_,DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, R, DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-delqre-onfalse">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,_,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, R,DATNRE,DAFNRE,DDTNRE,DDFNRE});

get_dests_operators(VHost, [{<<"x-addq!re-ontrue">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,_,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,R,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-addq!re-onfalse">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,DATNRE,_,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,DATNRE,R,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-delq!re-ontrue">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,DATNRE,DAFNRE,_,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,DATNRE,DAFNRE,R,DDFNRE});
get_dests_operators(VHost, [{<<"x-delq!re-onfalse">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,DATNRE,DAFNRE,DDTNRE,_}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,DATNRE,DAFNRE,DDTNRE,R});
get_dests_operators(VHost, [_ | T], Dests, DestsRE) ->
    get_dests_operators(VHost, T, Dests, DestsRE).


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
    BindingOrder = get_binding_order(BindingArgs, 200),
    {GOT, GOF} = get_goto_operators(BindingArgs, {0, 0}),
    StopOperators = get_stop_operators(BindingArgs, {0, 0}),
    FlattenedBindindArgs = flatten_binding_args(rebuild_args(BindingArgs, Dest)),
    MatchHKOps = get_match_hk_ops(FlattenedBindindArgs),
    MatchRKOps = get_match_rk_ops(FlattenedBindindArgs, []),
    MatchOps = {MatchHKOps, MatchRKOps},
    {{DAT, DAF, DDT, DDF}, {DATRE, DAFRE, DDTRE, DDFRE, DATNRE, DAFNRE, DDTNRE, DDFNRE}} = get_dests_operators(VHost, FlattenedBindindArgs),
    CurrentOrderedBindings = case mnesia:read(rabbit_open_bindings, XName, write) of
        [] -> [];
        [#open_bindings{bindings = E}] -> E
    end,
    NewBinding1 = {BindingOrder, BindingType, Dest, MatchOps},
    NewBinding2 = case {GOT, GOF, StopOperators, DAT, DAF, DDT, DDF, DATRE, DAFRE, DDTRE, DDFRE, DATNRE, DAFNRE, DDTNRE, DDFNRE} of
        {0, 0, {0, 0}, [], [], [], [], nil, nil, nil, nil, nil, nil, nil, nil} -> NewBinding1;
        {_, _, _, [], [], [], [], nil, nil, nil, nil, nil, nil, nil, nil} -> erlang:append_element(NewBinding1, {GOT, GOF, StopOperators});
        _ -> erlang:append_element(NewBinding1, {GOT, GOF, StopOperators, DAT, DAF, DDT, DDF, VHost, DATRE, DAFRE, DDTRE, DDFRE, DATNRE, DAFNRE, DDTNRE, DDFNRE})
    end,
    NewBinding = erlang:append_element(NewBinding2, BindingId),
    NewBindings = lists:keysort(1, [NewBinding | CurrentOrderedBindings]),
    NewRecord = #open_bindings{exchange_name = XName, bindings = NewBindings},
    ok = mnesia:write(rabbit_open_bindings, NewRecord, write);
add_binding(_, _, _) ->
    ok.

% mnesia table open_bindings doe not HAVE TO BE ordset?..

remove_bindings(transaction, #exchange{name = XName}, BindingsToDelete) ->
    CurrentOrderedBindings = case mnesia:read(rabbit_open_bindings, XName, write) of
        [] -> [];
        [#open_bindings{bindings = E}] -> E
    end,
    BindingIdsToDelete = [crypto:hash(md5, term_to_binary(B)) || B <- BindingsToDelete],
    NewOrderedBindings = remove_bindings_ids(BindingIdsToDelete, CurrentOrderedBindings, []),
    NewRecord = #open_bindings{exchange_name = XName, bindings = NewOrderedBindings},
    ok = mnesia:write(rabbit_open_bindings, NewRecord, write);
remove_bindings(_, _, _) ->
    ok.

remove_bindings_ids(_, [], Res) -> Res;
remove_bindings_ids(BindingIdsToDelete, [Bind = {_,_,_,_,_,BId} | T], Res) ->
    case lists:member(BId, BindingIdsToDelete) of
        true -> remove_bindings_ids(BindingIdsToDelete, T, Res);
        _    -> remove_bindings_ids(BindingIdsToDelete, T, lists:append(Res, [Bind]))
    end;
remove_bindings_ids(BindingIdsToDelete, [Bind = {_,_,_,_,BId} | T], Res) ->
    case lists:member(BId, BindingIdsToDelete) of
        true -> remove_bindings_ids(BindingIdsToDelete, T, Res);
        _    -> remove_bindings_ids(BindingIdsToDelete, T, lists:append(Res, [Bind]))
    end.


assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

