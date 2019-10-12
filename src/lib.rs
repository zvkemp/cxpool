// meta FIXME: this should be more like a GenServer.
// I.e., the ConnectionPool has an internal state, and a mailbox
// handle_cast with a checkout message should reply to a oneshot sender with a future
// that eventually resolves into a connection, either as the result of the connection being built,
// or as the result of another connection being returned to the pool.
//
use futures::channel::mpsc::{self, Sender, Receiver};
use futures::channel::oneshot;
use std::fmt::Debug;
use futures::sink::SinkExt;
use std::collections::HashMap;

pub struct ConnectionPool<T: 'static + Send + Debug> {
    size: usize,
    built: usize,
    connection_builder: Box<dyn Fn() -> T + Send>,
    queue_in: Sender<oneshot::Sender<Connection<T>>>,
    queue_out: Option<Receiver<oneshot::Sender<Connection<T>>>>, // FIXME: probably shouldn't belong to the struct?
    available_sender: Sender<T>,
    available_receiver: Option<Receiver<T>>, // FIXME: probably shouldn't belong to the struct?
    connections: HashMap<usize, Option<AvailableConnection<T>>>,
    waiter_receiver: Receiver<()>,
    waiter_sender: Sender<()>
}

#[derive(Debug)]
struct AvailableConnection<T: 'static + Send + Debug> {
    index: usize,
    inner: Option<T>,
    prepared: bool,
}

#[derive(Debug)]
pub struct Connection<T: 'static + Send + std::fmt::Debug> {
    inner: Option<T>,
    rubberband: Option<Sender<T>>,
    index: usize
}

impl<T: 'static + Send + std::fmt::Debug> Drop for Connection<T> {
    fn drop(&mut self) {
        let mut rubberband = self.rubberband.take().unwrap();
        let inner = self.inner.take().unwrap();

        async_std::task::spawn(async move {
            println!("returning connection: {:?}", inner);
            rubberband.send(inner).await.unwrap();
        });
    }
}

impl<T: 'static + Send + std::fmt::Debug> Connection<T> {
    pub fn inner_mut(&mut self) -> Result<&mut T, ()> {
        match self.inner {
            Some(ref mut x) => Ok(x),
            None => Err(())
        }
    }
}

// design:
//
// calling checkout returns a future; await returns a &mut connection
//
// the future polls until a connection is available
// do we need channels?
//
// internal:
//
// checkout sends a message to a queue,
// includes a oneshot channel that resolves into a connection (reference?)

// FIXME: better way to borrow connection?
// FIXME: count checkouts/checkins, or just let the await semantics handle it?
// FIXME: what happens if a connection is dismantled by the borrow task?
// FIXME: task-local connection checkout?
impl<T: Send + std::fmt::Debug + 'static> ConnectionPool<T> {

    pub async fn checkout(&mut self) -> std::result::Result<Connection<T>, futures::channel::oneshot::Canceled> {
        println!("checkout >>> ");
        let (tx, rx) = oneshot::channel::<Connection<T>>();

        self.queue_in.send(tx).await.unwrap();
        rx.await
    }

    fn build_connection(&mut self) -> () {
        self.built += 1;
        let index = self.built.clone();
        let cx = (self.connection_builder)();

        let connection = Connection {
            index,
            inner: Some(cx)
        }

        self.available_connections.insert(index, AvailableConnection { prepared: true, inner: None });
        println!("<<< connection built {:?}", connection);
        // FIXME: should block?
        async_std::task::block_on(async { self.available_sender.send(connection).await.unwrap() });
    }

    // FIXME: no async_std?
    fn spawn_server_loop(&mut self) -> () {
        use async_std::task;
        use futures::prelude::*;

        let mut queue_out = self.queue_out.take().unwrap();
        let mut available_connections = self.available_receiver.take().unwrap();
        let available_sender = self.available_sender.clone();
        let waiter_sender = self.waiter_sender.clone();

        task::spawn(async move {
            while let Some(tx) = queue_out.next().await {
                let cx = match available_connections.try_next() {
                    Ok(Some(t)) => { t },
                    Ok(None) => { // ?
                    },
                    Err(e) => {
                        println!("{:?}", e);
                        waiter_sender.send(()).await.unwrap();
                        available_connections.next().await.unwrap();
                    }
                }
                println!("sending connection to tx={:?}; cx={:?}", tx, cx);
                tx.send(Connection { inner: Some(cx), rubberband: Some(available_sender.clone()) }).unwrap();
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        let (tx, rx) = mpsc::channel(512); // FIXME: bound?
        let (available_sender, available_reciever) = mpsc::channel(512); // FIXME: bound to pool size!
        let (waiter_sender, waiter_receiver) = mpsc::channel(512);
        let mut pool: ConnectionPool<Vec<u8>> = ConnectionPool {
            size: 10,
            built: 0,
            connection_builder: Box::new(|| vec![1u8]),
            queue_in: tx,
            queue_out: Some(rx),
            available_sender,
            available_receiver: Some(available_reciever),
            waiter_sender,
            waiter_receiver,
        };

        pool.build_connection();
        pool.spawn_server_loop();

        use futures::future::FutureExt;
        {
            let mut c = async_std::task::block_on(async { pool.checkout().await.unwrap() });
            c.inner_mut().map(|x| x.push(2)).unwrap();
        }

        let mut d = async_std::task::block_on(async { pool.checkout().await.unwrap() });
        d.inner_mut().map(|x| x.push(3)).unwrap();
        println!("checkout2: {:?}", d);
    }
}
