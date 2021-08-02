-module(blockchain_txn_bundle_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").
-include_lib("blockchain/include/blockchain_vars.hrl").

-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([
         basic_test/1,
         negative_test/1,
         double_spend_test/1,
         successive_test/1,
         invalid_successive_test/1,
         single_payer_test/1,
         single_payer_invalid_test/1,
         full_circle_test/1,
         add_assert_test/1,
         invalid_add_assert_test/1,
         single_txn_bundle_test/1,
         bundleception_test/1
        ]).

%% Setup ----------------------------------------------------------------------

all() -> [
          basic_test,
          negative_test,
          double_spend_test,
          successive_test,
          invalid_successive_test,
          single_payer_test,
          single_payer_invalid_test,
          full_circle_test,
          add_assert_test,
          invalid_add_assert_test,
          single_txn_bundle_test,
          bundleception_test
         ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_TestCase, Cfg) ->
    t_chain:start(Cfg).

end_per_testcase(_TestCase, _Cfg) ->
    t_chain:stop().

%% Cases ----------------------------------------------------------------------

basic_test(Cfg) ->
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),
    [Src | _] = ?config(users_with_hnt, Cfg),
    Dst = t_user:new(),

    SrcBalance0 = t_chain:get_balance(Chain, Src),
    DstBalance0 = t_chain:get_balance(Chain, Dst),

    %% Expected initial balances:
    ?assertEqual(5000, SrcBalance0),
    ?assertEqual(   0, DstBalance0),

    AmountPerTxn = 1000,
    Txns =
        [
            t_txn:pay(Src, Dst, AmountPerTxn, 1),
            t_txn:pay(Src, Dst, AmountPerTxn, 2)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),
    ?assertMatch(ok, t_chain:commit(Chain, ConsensusMembers, [TxnBundle])),

    AmountTotal = length(Txns) * AmountPerTxn,
    ?assertEqual(SrcBalance0 - AmountTotal, t_chain:get_balance(Chain, Src)),
    ?assertEqual(DstBalance0 + AmountTotal, t_chain:get_balance(Chain, Dst)),

    ok.

negative_test(Cfg) ->
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),
    [Src | _] = ?config(users_with_hnt, Cfg),
    SrcBalance0 = t_chain:get_balance(Chain, Src),

    %% Dst has no need for a starting balance, so we use an arbitrary address.
    Dst = t_user:new(),
    DstBalance0 = t_chain:get_balance(Chain, Dst),

    %% Expected initial balances:
    ?assertEqual(5000, SrcBalance0),
    ?assertEqual(   0, DstBalance0),

    AmountPerTxn = 1000,
    Txns =
        %% Reversed order of nonces, invalidating the bundle:
        [
            t_txn:pay(Src, Dst, AmountPerTxn, 2),
            t_txn:pay(Src, Dst, AmountPerTxn, 1)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(
        {error, {invalid_txns, [{_, invalid_bundled_txns}]}},
        t_chain:commit(Chain, ConsensusMembers, [TxnBundle])
    ),

    %% Balances should not have changed since the bundle was invalid:
    ?assertEqual(SrcBalance0, t_chain:get_balance(Chain, Src)),
    ?assertEqual(DstBalance0, t_chain:get_balance(Chain, Dst)),

    ok.

double_spend_test(Cfg) ->
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),
    [Src | _] = ?config(users_with_hnt, Cfg),
    SrcBalance0 = t_chain:get_balance(Chain, Src),
    SrcNonce = 1,

    %% A destination can be arbitrary, since it has no need for a starting balance.
    %% A destination can be dead-end (no priv key), since we'll never pay from it.
    Dst1 = t_user:new(),
    Dst2 = t_user:new(),
    Dst1Balance0 = t_chain:get_balance(Chain, Dst1),
    Dst2Balance0 = t_chain:get_balance(Chain, Dst2),

    %% Expected initial balances:
    ?assertEqual(5000, SrcBalance0),
    ?assertEqual(   0, Dst1Balance0),
    ?assertEqual(   0, Dst2Balance0),

    AmountPerTxn = 1000,
    Txns =
        [
            %% good txn: first spend
            t_txn:pay(Src, Dst1, AmountPerTxn, SrcNonce),

            %% bad txn: double-spend = same nonce, diff dst
            t_txn:pay(Src, Dst2, AmountPerTxn, SrcNonce)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(
        {error, {invalid_txns, [{_, invalid_bundled_txns}]}},
        t_chain:commit(Chain, ConsensusMembers, [TxnBundle])
    ),

    %% All balances remain, since all txns were rejected, not just the bad one.
    ?assertEqual(SrcBalance0 , t_chain:get_balance(Chain, Src)),
    ?assertEqual(Dst1Balance0, t_chain:get_balance(Chain, Dst1)),
    ?assertEqual(Dst2Balance0, t_chain:get_balance(Chain, Dst2)),

    ok.

successive_test(Cfg) ->
    %% Test a successive valid bundle payment
    %% A -> B
    %% B -> C
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),

    %% A needs a starting balance,
    %% B and C can be arbitrary, since they have no need for a starting balance
    %% - all funds will originate from A.
    [A | _] = ?config(users_with_hnt, Cfg),
    B = t_user:new(),
    C = t_user:new(),

    A_Balance0 = t_chain:get_balance(Chain, A),
    B_Balance0 = t_chain:get_balance(Chain, B),
    C_Balance0 = t_chain:get_balance(Chain, C),

    %% Expected initial balances:
    ?assertEqual(5000, A_Balance0),
    ?assertEqual(   0, B_Balance0),
    ?assertEqual(   0, C_Balance0),

    AmountAToB = A_Balance0,
    AmountBToC = AmountAToB - 1,
    Txns =
        [
            t_txn:pay(A, B, AmountAToB, 1),
            t_txn:pay(B, C, AmountBToC, 1)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(ok, t_chain:commit(Chain, ConsensusMembers, [TxnBundle])),
    ?assertEqual(A_Balance0 - AmountAToB             , t_chain:get_balance(Chain, A)),
    ?assertEqual(B_Balance0 + AmountAToB - AmountBToC, t_chain:get_balance(Chain, B)),
    ?assertEqual(C_Balance0 + AmountBToC             , t_chain:get_balance(Chain, C)),

    ok.

invalid_successive_test(Cfg) ->
    %% Test a successive invalid bundle payment
    %% A -> B
    %% B -> C <-- this is invalid
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),

    %% A needs a starting balance,
    %% B and C can be arbitrary, since they have no need for a starting balance
    %% - all funds will originate from A.
    [A | _] = ?config(users_with_hnt, Cfg),
    B = t_user:new(),
    C = t_user:new(),

    A_Balance0 = t_chain:get_balance(Chain, A),
    B_Balance0 = t_chain:get_balance(Chain, B),
    C_Balance0 = t_chain:get_balance(Chain, C),

    %% Expected initial balances:
    ?assertEqual(5000, A_Balance0),
    ?assertEqual(   0, B_Balance0),
    ?assertEqual(   0, C_Balance0),

    AmountAToB = A_Balance0,
    AmountBToC = B_Balance0 + AmountAToB + 1,  % overdraw attempt
    Txns =
        [
            t_txn:pay(A, B, AmountAToB, 1),
            t_txn:pay(B, C, AmountBToC, 1)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(
        {error, {invalid_txns, [{_, invalid_bundled_txns}]}},
        t_chain:commit(Chain, ConsensusMembers, [TxnBundle])
    ),
    ?assertEqual(A_Balance0, t_chain:get_balance(Chain, A)),
    ?assertEqual(B_Balance0, t_chain:get_balance(Chain, B)),
    ?assertEqual(C_Balance0, t_chain:get_balance(Chain, C)),

    ok.

single_payer_test(Cfg) ->
    %% Test a bundled payment from single payer
    %% A -> B
    %% A -> C
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),

    %% A needs a starting balance,
    %% B and C can be arbitrary,
    %% since they have no need for a starting balance
    %% - all funds will originate from A.
    [A | _] = ?config(users_with_hnt, Cfg),
    B = t_user:new(),
    C = t_user:new(),

    A_Balance0 = t_chain:get_balance(Chain, A),
    B_Balance0 = t_chain:get_balance(Chain, B),
    C_Balance0 = t_chain:get_balance(Chain, C),

    %% Expected initial values:
    ?assertEqual(5000, A_Balance0),
    ?assertEqual(   0, B_Balance0),
    ?assertEqual(   0, C_Balance0),

    AmountAToB = 2000,
    AmountAToC = 3000,
    Txns =
        [
            t_txn:pay(A, B, AmountAToB, 1),
            t_txn:pay(A, C, AmountAToC, 2)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(ok, t_chain:commit(Chain, ConsensusMembers, [TxnBundle])),
    ?assertEqual(A_Balance0 - AmountAToB - AmountAToC, t_chain:get_balance(Chain, A)),
    ?assertEqual(B_Balance0 + AmountAToB             , t_chain:get_balance(Chain, B)),
    ?assertEqual(C_Balance0 + AmountAToC             , t_chain:get_balance(Chain, C)),

    ok.

single_payer_invalid_test(Cfg) ->
    %% Test a bundled payment from single payer
    %% Given:
    %%   N < (K + M)
    %%   A: N
    %%   B: 0
    %% Attempt:
    %%   A -M-> B : OK
    %%   A -K-> C : insufficient funds
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),

    %% A needs a starting balance,
    %% B and C can be arbitrary, since they have no need for a starting balance
    %% - all funds will originate from A.
    [A | _] = ?config(users_with_hnt, Cfg),
    B = t_user:new(),
    C = t_user:new(),

    A_Balance0 = t_chain:get_balance(Chain, A),
    B_Balance0 = t_chain:get_balance(Chain, B),
    C_Balance0 = t_chain:get_balance(Chain, C),

    %% Expected initial values:
    ?assertEqual(5000, A_Balance0),
    ?assertEqual(   0, B_Balance0),
    ?assertEqual(   0, C_Balance0),

    Overage = 1000,
    AmountAToB = 2000,
    AmountAToC = (A_Balance0 - AmountAToB) + Overage,

    % Sanity checks
    ?assert(A_Balance0 >= AmountAToB),
    ?assert(A_Balance0 <  (AmountAToB + AmountAToC)),

    Txns =
        [
            t_txn:pay(A, B, AmountAToB, 1),
            t_txn:pay(A, C, AmountAToC, 2)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(
        {error, {invalid_txns, [{_, invalid_bundled_txns}]}},
        t_chain:commit(Chain, ConsensusMembers, [TxnBundle])
    ),

    %% Because of one invalid txn (A->C), the whole bundle was rejected, so
    %% nothing changed:
    ?assertEqual(A_Balance0, t_chain:get_balance(Chain, A)),
    ?assertEqual(B_Balance0, t_chain:get_balance(Chain, B)),
    ?assertEqual(C_Balance0, t_chain:get_balance(Chain, C)),

    ok.

full_circle_test(Cfg) ->
    %% Test a full-circle transfer of funds:
    %% Given:
    %%   A: NA
    %%   B: NB
    %%   C: NC
    %% Attempt:
    %%   A -(NA      )-> B
    %%   B -(NA+NB   )-> C
    %%   C -(NA+NB+NC)-> A
    %% Expect:
    %%   A: NA + NB + NC
    %%   B: 0
    %%   C: 0
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),

    %% A needs a starting balance,
    %% B and C can be arbitrary, since they have no need for a starting balance
    %% - all funds will originate from A.
    [A | _] = ?config(users_with_hnt, Cfg),
    B = t_user:new(),
    C = t_user:new(),

    A_Balance0 = t_chain:get_balance(Chain, A),
    B_Balance0 = t_chain:get_balance(Chain, B),
    C_Balance0 = t_chain:get_balance(Chain, C),

    %% Expected initial values:
    ?assertEqual(5000, A_Balance0),
    ?assertEqual(   0, B_Balance0),
    ?assertEqual(   0, C_Balance0),

    AmountAToB = A_Balance0,
    AmountBToC = A_Balance0 + B_Balance0,
    AmountCToA = A_Balance0 + B_Balance0 + C_Balance0,
    BalanceExpectedA = AmountCToA,
    BalanceExpectedB = 0,
    BalanceExpectedC = 0,

    Txns =
        [
            t_txn:pay(A, B, AmountAToB, 1),
            t_txn:pay(B, C, AmountBToC, 1),
            t_txn:pay(C, A, AmountCToA, 1)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(ok, t_chain:commit(Chain, ConsensusMembers, [TxnBundle])),
    ?assertEqual(BalanceExpectedA, t_chain:get_balance(Chain, A)),
    ?assertEqual(BalanceExpectedB, t_chain:get_balance(Chain, B)),
    ?assertEqual(BalanceExpectedC, t_chain:get_balance(Chain, C)),

    ok.

add_assert_test(Cfg) ->
    %% Test add + assert in a bundled txn
    %% A -> [add_gateway, assert_location]

    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),
    [Owner | _] = ?config(users_with_dc, Cfg), % Need DC to own a gateway
    Gateway = t_user:new(),

    LocationIndex = 631210968910285823,
    Txns =
        [
            t_txn:gateway_add(Owner, Gateway),
            t_txn:assert_location(Owner, Gateway, LocationIndex, 1)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ActiveGateways0 = t_chain:get_active_gateways(Chain),
    ?assertMatch(ok, t_chain:commit(Chain, ConsensusMembers, [TxnBundle])),
    ActiveGateways1 = t_chain:get_active_gateways(Chain),

    ?assertEqual(
        maps:size(ActiveGateways0) + 1,
        maps:size(ActiveGateways1)
    ),

    %% Check that it has the correct location
    AddedGw = maps:get(t_user:addr(Gateway), ActiveGateways1),
    GwLoc = blockchain_ledger_gateway_v2:location(AddedGw),
    ?assertEqual(GwLoc, LocationIndex),

    ok.

invalid_add_assert_test(Cfg) ->
    %% Test assert+add in a bundled txn
    %% A -> [assert_location, add_gateway]
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),
    [Owner | _] = ?config(users_with_dc, Cfg), % Need DC to own a gateway
    Gateway = t_user:new(),

    LocationIndex = 631210968910285823,
    Txns =
        [
            t_txn:assert_location(Owner, Gateway, LocationIndex, 1),
            t_txn:gateway_add(Owner, Gateway)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ActiveGateways0 = t_chain:get_active_gateways(Chain),
    ?assertMatch(
        {error, {invalid_txns, [{_, invalid_bundled_txns}]}},
        t_chain:commit(Chain, ConsensusMembers, [TxnBundle])
    ),
    ActiveGateways1 = t_chain:get_active_gateways(Chain),

    %% Check that the gateway did not get added
    ?assertEqual(
        maps:size(ActiveGateways0),
        maps:size(ActiveGateways1)
    ),

    ok.

single_txn_bundle_test(Cfg) ->
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),

    %% Src needs a starting balance,
    %% Dst has no need for a starting balance, so we use an arbitrary address.
    [Src | _] = ?config(users_with_hnt, Cfg),
    Dst = t_user:new(),

    SrcBalance0 = t_chain:get_balance(Chain, Src),
    DstBalance0 = t_chain:get_balance(Chain, Dst),

    %% Expected initial balances:
    ?assertEqual(5000, SrcBalance0),
    ?assertEqual(   0, DstBalance0),

    Amount = 1000,
    Txns = [t_txn:pay(Src, Dst, Amount, 1)],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    %% The bundle is invalid since it does not contain atleast two txns in it
    ?assertMatch(
        {error, {invalid_txns, [{_, invalid_min_bundle_size}]}},
        t_chain:commit(Chain, ConsensusMembers, [TxnBundle])
    ),

    %% Sanity check that balances haven't changed:
    ?assertEqual(SrcBalance0, t_chain:get_balance(Chain, Src)),
    ?assertEqual(DstBalance0, t_chain:get_balance(Chain, Dst)),

    ok.

bundleception_test(Cfg) ->
    ConsensusMembers = ?config(users_in_consensus, Cfg),
    Chain = ?config(chain, Cfg),
    [Src | _] = ?config(users_with_hnt, Cfg),
    Dst = t_user:new(),
    SrcBalance0 = t_chain:get_balance(Chain, Src),
    DstBalance0 = t_chain:get_balance(Chain, Dst),

    ?assertEqual(5000, SrcBalance0),
    ?assertEqual(   0, DstBalance0),

    AmountPerTxn = 1000,
    TxnBundle1 =
        blockchain_txn_bundle_v1:new(
            [
                t_txn:pay(Src, Dst, AmountPerTxn, 1),
                t_txn:pay(Src, Dst, AmountPerTxn, 2)
            ]
        ),
    TxnBundle2 =
        blockchain_txn_bundle_v1:new(
            [
                t_txn:pay(Src, Dst, AmountPerTxn, 3),
                t_txn:pay(Src, Dst, AmountPerTxn, 4)
            ]
        ),
    TxnBundleCeption = blockchain_txn_bundle_v1:new([TxnBundle1, TxnBundle2]),

    ?assertMatch(
        {error, {invalid_txns, [{_, invalid_bundleception}]}},
        t_chain:commit(Chain, ConsensusMembers, [TxnBundleCeption])
    ),

    ?assertEqual(SrcBalance0, t_chain:get_balance(Chain, Src)),
    ?assertEqual(DstBalance0, t_chain:get_balance(Chain, Dst)),

    ok.

%% Helpers --------------------------------------------------------------------
