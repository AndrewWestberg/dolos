use std::sync::Arc;

use dolos_core::{
    ChainPoint, Domain, EraCbor, MempoolStore, StateStore, StateWriter, TxoRef, UtxoSetDelta,
};
use dolos_testing::toy_domain::ToyDomain;
use pallas::{
    codec::{
        minicbor,
        utils::{CborWrap, KeepRaw},
    },
    crypto::hash::Hash,
    ledger::{
        addresses::{Address, Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart},
        primitives::conway::{
            DatumOption, PlutusScript, PostAlonzoTransactionOutput, TransactionOutput, Value,
        },
        traverse::ComputeHash,
    },
    txbuilder::{BuildConway, ExUnits, Input, Output, ScriptKind, StagingTransaction},
};

fn unsigned_script_transaction(succeeds: bool) -> (ToyDomain, Vec<u8>, TxoRef) {
    // UPLC 1.0.0: (lam ctx (con unit ())) / (lam ctx (error)).
    let script = hex::decode(if succeeds {
        "450100002499"
    } else {
        "450100002601"
    })
    .unwrap();
    let script_hash = PlutusScript::<3>(script.clone().into()).compute_hash();
    let address = Address::Shelley(ShelleyAddress::new(
        Network::Testnet,
        ShelleyPaymentPart::Script(script_hash),
        ShelleyDelegationPart::Null,
    ));
    let input = TxoRef(Hash::from([1; 32]), 0);
    let output = TransactionOutput::PostAlonzo(KeepRaw::from(PostAlonzoTransactionOutput {
        address: address.to_vec().into(),
        value: Value::Coin(2_000_000),
        datum_option: Some(KeepRaw::from(DatumOption::Data(CborWrap(
            minicbor::decode(&[0]).unwrap(),
        )))),
        script_ref: None,
    }));
    let mut genesis = dolos_cardano::include::devnet::load();
    genesis.force_protocol = Some(9);
    let domain = ToyDomain::new_with_genesis(
        Arc::new(genesis),
        Some(UtxoSetDelta {
            produced_utxo: [(
                input.clone(),
                Arc::new(EraCbor(7, minicbor::to_vec(output).unwrap())),
            )]
            .into(),
            ..Default::default()
        }),
        None,
    );
    let writer = domain.state().start_writer().unwrap();
    writer.set_cursor(ChainPoint::Slot(0)).unwrap();
    writer.commit().unwrap();
    let tx_input = Input::new(input.0, input.1.into());
    let transaction = StagingTransaction::new()
        .input(tx_input.clone())
        .output(Output::new(address, 1_500_000))
        .fee(500_000)
        .disclosed_signer(Hash::from([2; 28]))
        .script(ScriptKind::PlutusV3, script)
        .add_spend_redeemer(tx_input, vec![0], Some(ExUnits { mem: 0, steps: 0 }))
        .build_conway_raw()
        .unwrap();
    (domain, transaction.tx_bytes.0, input)
}

#[tokio::test]
async fn unsigned_evaluation_preserves_script_errors_and_submission_validation() {
    macro_rules! check_version {
        ($version:ident) => {{
            use pallas::interop::utxorpc::$version::spec::{
                cardano,
                submit::{self, submit_service_server::SubmitService},
            };
            for succeeds in [true, false] {
                let (domain, cbor, input) = unsigned_script_transaction(succeeds);
                let original = domain.state().get_utxos(vec![input.clone()]).unwrap();
                let service = super::$version::submit::SubmitServiceImpl::new(domain.clone());
                let raw = submit::AnyChainTx {
                    r#type: Some(submit::any_chain_tx::Type::Raw(cbor.clone().into())),
                };
                let response = service
                    .eval_tx(tonic::Request::new(submit::EvalTxRequest {
                        tx: Some(raw.clone()),
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                let Some(submit::any_chain_eval::Chain::Cardano(report)) =
                    response.report.unwrap().chain
                else {
                    panic!("missing evaluation")
                };
                assert_eq!(report.errors.len(), usize::from(!succeeds), "{report:?}");
                let [redeemer] = report.redeemers.as_slice() else {
                    panic!("{report:?}")
                };
                assert_eq!(redeemer.purpose, cardano::RedeemerPurpose::Spend as i32);
                assert_eq!(redeemer.index, 0);
                let units = redeemer.ex_units.as_ref().unwrap();
                assert!(units.memory > 0 && units.steps > 0);
                let error = service
                    .submit_tx(tonic::Request::new(submit::SubmitTxRequest {
                        tx: Some(raw),
                    }))
                    .await
                    .unwrap_err();
                assert_eq!(error.code(), tonic::Code::InvalidArgument);
                assert!(!domain.mempool().has_pending());
                assert_eq!(domain.state().get_utxos(vec![input]).unwrap(), original);
            }
        }};
    }
    check_version!(v1alpha);
    check_version!(v1beta);
}
