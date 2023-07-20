use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures::{
    channel::{mpsc, oneshot},
    FutureExt, Sink, Stream, StreamExt,
};
use tracing::trace;

pub struct Receiver<T> {
    txer_tx: mpsc::Sender<oneshot::Sender<T>>,
    active_rxer: Option<oneshot::Receiver<T>>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            txer_tx: self.txer_tx.clone(),
            active_rxer: None,
        }
    }
}

impl<T> Stream for Receiver<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<T>> {
        Poll::Ready(loop {
            if let Some(rxer) = &mut self.active_rxer {
                match ready!(rxer.poll_unpin(cx)) {
                    Ok(value) => {
                        self.active_rxer = None;
                        break Some(value);
                    }
                    Err(_) => self.active_rxer = None,
                }
            }

            assert!(self.active_rxer.is_none());
            match ready!(self.txer_tx.poll_ready(cx)) {
                Ok(()) => {
                    let (txer, rxer) = oneshot::channel();
                    self.txer_tx.start_send(txer).unwrap();
                    self.active_rxer = Some(rxer);
                }
                Err(_) => break None,
            }
        })
    }
}

#[derive(Debug)]
pub enum SendError {
    Closed,
}

pub struct Sender<T> {
    txer_rx: mpsc::Receiver<oneshot::Sender<T>>,
    active_txer: Option<oneshot::Sender<T>>,
    output_buf: Option<T>,
}

impl<T> Unpin for Sender<T> {}

impl<T> Sink<T> for Sender<T> {
    type Error = SendError;

    #[tracing::instrument(skip(self, cx))]
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(if self.active_txer.is_some() {
            trace!("txer is ready");
            Ok(())
        } else {
            trace!("polling for txer");
            match ready!(self.txer_rx.poll_next_unpin(cx)) {
                Some(txer) => {
                    trace!("got txer!");
                    self.active_txer = Some(txer);
                    Ok(())
                }
                None => Err(SendError::Closed),
            }
        })
    }

    #[tracing::instrument(skip(self, item))]
    fn start_send(mut self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        assert!(self.output_buf.is_none());
        self.output_buf = Some(item);
        Ok(())
    }

    #[tracing::instrument(skip(self, cx))]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            if self.output_buf.is_some() {
                trace!("has output buffer, grabbing txer");
                ready!(self.as_mut().poll_ready(cx)?);
                trace!("got txer, trying to send");
                match self
                    .active_txer
                    .take()
                    .unwrap()
                    .send(self.output_buf.take().unwrap())
                {
                    Ok(()) => trace!("tx successful, yay!"),
                    Err(output_buf) => {
                        trace!("tx failed, requeueing");
                        self.output_buf = Some(output_buf)
                    }
                }
            } else {
                break Poll::Ready(Ok(()));
            }
        }
    }

    #[tracing::instrument(skip(self, cx))]
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx)?);
        self.txer_rx.close();
        self.active_txer = None;
        Poll::Ready(Ok(()))
    }
}

pub fn channel<T>(buffer: usize) -> (Sender<T>, Receiver<T>) {
    let (txer_tx, txer_rx) = mpsc::channel(buffer);
    (
        Sender {
            txer_rx,
            active_txer: None,
            output_buf: None,
        },
        Receiver {
            txer_tx,
            active_rxer: None,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use futures::{future, poll, stream, SinkExt, StreamExt, TryStreamExt};
    use once_cell::sync::Lazy;
    use tracing::{info, info_span, Instrument};
    use tracing_subscriber::EnvFilter;

    use crate::channel;

    static INIT_LOG: Lazy<()> = Lazy::new(|| {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init()
    });

    #[tokio::test]
    async fn test_send_message() {
        Lazy::force(&INIT_LOG);
        async {
            let (mut tx, rx) = channel(5);
            info!("starting rxer");
            let rx = tokio::spawn(
                rx.inspect(|msg| info!(msg, "got message!"))
                    .collect::<Vec<_>>()
                    .in_current_span(),
            );
            info!("sending message");
            tx.send(1).await.unwrap();
            info!("sent! waiting for rxer");
            drop(tx);
            assert_eq!(rx.await.unwrap(), vec![1]);
        }
        .instrument(info_span!("test_send_message"))
        .await
    }

    #[tokio::test]
    async fn test_send_several_messages() {
        Lazy::force(&INIT_LOG);
        async {
            let (mut tx, rx) = channel(5);
            info!("starting rxer");
            let rx = tokio::spawn(
                rx.inspect(|msg| info!(msg, "got message!"))
                    .collect::<HashSet<_>>()
                    .in_current_span(),
            );
            info!("sending message");
            for i in 0..1000 {
                tx.send(i).await.unwrap();
            }
            info!("sent! waiting for rxer");
            drop(tx);
            assert_eq!(rx.await.unwrap(), (0..1000).collect::<HashSet<_>>());
        }
        .instrument(info_span!("test_send_several_messages"))
        .await
    }

    #[tokio::test]
    async fn test_send_messages_to_several_receivers() {
        Lazy::force(&INIT_LOG);
        async {
            let (mut tx, rx) = channel(5);
            info!("starting rxers");
            let rxers = (0..10)
                .map(|rxer_i| {
                    tokio::spawn(
                        rx.clone()
                            .inspect(move |msg| info!(rxer_i, msg, "got message!"))
                            .collect::<HashSet<_>>()
                            .in_current_span(),
                    )
                })
                .collect::<Vec<_>>();
            info!("sending message");
            for i in 0..1000 {
                tx.send(i).await.unwrap();
            }
            info!("sent! waiting for rxers");
            drop(tx);
            let rxers = stream::iter(rxers)
                .buffer_unordered(10)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert!(rxers.iter().all(|rxer| !rxer.is_empty()));
            assert_eq!(
                rxers.iter().flatten().copied().collect::<HashSet<_>>(),
                (0..1000).collect::<HashSet<_>>()
            );
        }
        .instrument(info_span!("test_send_messages_to_several_receivers"))
        .await
    }

    #[tokio::test]
    async fn test_send_to_dropped_rxer() {
        Lazy::force(&INIT_LOG);
        async {
            let (mut tx, mut rx) = channel(5);
            let mut tmp_rx = rx.clone();
            // init tmp_rx to wait for a message
            assert!(poll!(tmp_rx.next()).is_pending());
            assert!(tmp_rx.active_rxer.is_some());
            assert!(rx.active_rxer.is_none());
            // let tx buffer up a message to send
            future::poll_fn(|cx| tx.poll_ready_unpin(cx)).await.unwrap();
            tx.start_send_unpin(1).unwrap();
            // drop tmp_rx before the message has been received
            drop(tmp_rx);
            // confirm that the message was instead received by rx once flushed
            let rxed = tokio::spawn(async move { rx.next().await }.in_current_span());
            tx.close().await.unwrap();
            assert_eq!(rxed.await.unwrap(), Some(1));
        }
        .instrument(info_span!("test_send_to_dropped_rxer"))
        .await
    }
}
