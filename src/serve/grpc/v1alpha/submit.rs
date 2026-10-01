use any_chain_eval::Chain;
use dolos_core::{EvalReport, SubmitExt};
use futures_core::Stream;
use futures_util::{StreamExt as _, TryStreamExt as _};
use pallas::crypto::hash::Hash;
use pallas::interop::utxorpc::v1alpha::spec::cardano::ExUnits;
use pallas::interop::utxorpc::v1alpha::spec::submit::{WaitForTxResponse, *};
use pallas::interop::utxorpc::v1alpha::{spec as u5c, Mapper};
use pallas::interop::utxorpc::LedgerContext;
use std::collections::HashSet;
use std::pin::Pin;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::prelude::*;

pub struct SubmitServiceImpl<D>
where
    D: Domain + LedgerContext,
{
    domain: D,
    mapper: Mapper<D>,
}

impl<D> SubmitServiceImpl<D>
where
    D: Domain + LedgerContext,
{
    pub fn new(domain: D) -> Self {
        let mapper = Mapper::new(domain.clone());
        Self { domain, mapper }
    }
}

fn tx_stage_to_u5c(stage: MempoolTxStage) -> i32 {
    match stage {
        MempoolTxStage::Pending => Stage::Mempool as i32,
        MempoolTxStage::Propagated => Stage::Network as i32,
        MempoolTxStage::Acknowledged => Stage::Acknowledged as i32,
        MempoolTxStage::Confirmed => Stage::Confirmed as i32,
        _ => Stage::Unspecified as i32,
    }
}

fn event_to_watch_mempool_response(event: MempoolEvent) -> WatchMempoolResponse {
    WatchMempoolResponse {
        tx: TxInMempool {
            r#ref: event.tx.hash.to_vec().into(),
            native_bytes: event.tx.payload.cbor().to_vec().into(),
            stage: tx_stage_to_u5c(event.tx.stage.clone()),
            parsed_state: None, // TODO
        }
        .into(),
    }
}

fn event_to_wait_for_tx_response(event: MempoolEvent) -> WaitForTxResponse {
    WaitForTxResponse {
        stage: tx_stage_to_u5c(event.tx.stage.clone()),
        r#ref: event.tx.hash.to_vec().into(),
    }
}

fn tx_eval_to_u5c<C: LedgerContext>(
    eval: Result<EvalReport, DomainError>,
    mapper: &Mapper<C>,
) -> u5c::cardano::TxEval {
    match eval {
        Ok(report) => u5c::cardano::TxEval {
            ex_units: report
                .iter()
                .try_fold(u5c::cardano::ExUnits::default(), |acc, eval| {
                    Some(ExUnits {
                        steps: acc.steps + eval.units.steps,
                        memory: acc.memory + eval.units.mem,
                    })
                }),
            redeemers: report
                .iter()
                .map(|x| u5c::cardano::Redeemer {
                    purpose: mapper.map_purpose(&x.tag).into(),
                    index: x.index,
                    ex_units: Some(u5c::cardano::ExUnits {
                        steps: x.units.steps,
                        memory: x.units.mem,
                    }),
                    ..Default::default()
                })
                .collect(),
            fee: None,      // TODO
            traces: vec![], // TODO
            errors: report
                .iter()
                .filter(|eval| !eval.success)
                .map(|eval| u5c::cardano::EvalError {
                    msg: format!(
                        "phase-2 script rejected redeemer {:?}/{}: {:?}; {:?}",
                        eval.tag, eval.index, eval.failure_message, eval.logs
                    ),
                })
                .collect(),
        },
        Err(e) => u5c::cardano::TxEval {
            errors: vec![u5c::cardano::EvalError {
                msg: format!("{e:#?}"),
            }],
            ..Default::default()
        },
    }
}

#[async_trait::async_trait]
impl<D> submit_service_server::SubmitService for SubmitServiceImpl<D>
where
    D: Domain + LedgerContext,
{
    type WaitForTxStream =
        Pin<Box<dyn Stream<Item = Result<WaitForTxResponse, tonic::Status>> + Send + 'static>>;

    type WatchMempoolStream =
        Pin<Box<dyn Stream<Item = Result<WatchMempoolResponse, tonic::Status>> + Send + 'static>>;

    async fn submit_tx(
        &self,
        request: Request<SubmitTxRequest>,
    ) -> Result<Response<SubmitTxResponse>, Status> {
        let message = request.into_inner();

        info!("received new grpc submit tx request: {:?}", message);

        let chain = self.domain.read_chain();

        let tx = message
            .tx
            .ok_or_else(|| Status::invalid_argument("missing tx"))?;
        let tx_bytes = match tx.r#type {
            Some(any_chain_tx::Type::Raw(bytes)) => bytes,
            _ => return Err(Status::invalid_argument("missing or unsupported tx type")),
        };

        let hash = self
            .domain
            .receive_tx("grpc", &chain, tx_bytes.as_ref())
            .map_err(|e| Status::invalid_argument(format!("could not process tx: {e}")))?;

        Ok(Response::new(SubmitTxResponse {
            r#ref: hash.to_vec().into(),
        }))
    }

    async fn wait_for_tx(
        &self,
        request: Request<WaitForTxRequest>,
    ) -> Result<Response<Self::WaitForTxStream>, Status> {
        let subjects: HashSet<_> = request
            .into_inner()
            .r#ref
            .into_iter()
            .map(|x| Hash::from(x.as_ref()))
            .collect();

        let initial_stages: Vec<_> = subjects
            .iter()
            .map(|x| {
                Result::<_, Status>::Ok(WaitForTxResponse {
                    stage: tx_stage_to_u5c(self.domain.mempool().check_status(x).stage),
                    r#ref: x.to_vec().into(),
                })
            })
            .collect();

        let updates = self.domain.mempool().subscribe();

        let updates = UpdateFilter::<D::Mempool>::new(updates, subjects)
            .map(|x| Ok(event_to_wait_for_tx_response(x)))
            .boxed();

        let stream = tokio_stream::iter(initial_stages).chain(updates).boxed();

        Ok(Response::new(stream))
    }

    async fn read_mempool(
        &self,
        _request: tonic::Request<ReadMempoolRequest>,
    ) -> Result<tonic::Response<ReadMempoolResponse>, tonic::Status> {
        Err(Status::unimplemented("read_mempool is not yet available"))
    }

    async fn watch_mempool(
        &self,
        _request: tonic::Request<WatchMempoolRequest>,
    ) -> Result<tonic::Response<Self::WatchMempoolStream>, tonic::Status> {
        let updates = self.domain.mempool().subscribe();

        let stream = updates
            .map_ok(event_to_watch_mempool_response)
            .map_err(|e| Status::internal(e.to_string()))
            .boxed();

        Ok(Response::new(stream))
    }

    async fn eval_tx(
        &self,
        request: tonic::Request<EvalTxRequest>,
    ) -> Result<tonic::Response<EvalTxResponse>, tonic::Status> {
        let tx = request
            .into_inner()
            .tx
            .ok_or_else(|| Status::invalid_argument("missing tx"))?;

        let tx_raw = match tx.r#type {
            Some(any_chain_tx::Type::Raw(bytes)) => bytes.to_vec(),
            _ => return Err(Status::invalid_argument("missing or unsupported tx type")),
        };

        let chain = self.domain.read_chain();

        let result = self.domain.evaluate_tx(&chain, &tx_raw);
        let result = tx_eval_to_u5c(result, &self.mapper);

        let report = AnyChainEval {
            chain: Some(Chain::Cardano(result)),
        };

        Ok(Response::new(EvalTxResponse {
            report: Some(report),
        }))
    }
}
