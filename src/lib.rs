use futures::channel::mpsc::{self, Sender, Receiver};
use futures::channel::oneshot;
pub struct ConnectionPool<T: Send> {
    size: usize,
    built: usize,
    connection_builder: Box<dyn Fn() -> T + Send>,
    connections: Vec<T>,
    queue_in: Sender<oneshot::Sender<T>>,
    queue_out: Option<Receiver<oneshot::Sender<T>>>, // FIXME: probably shouldn't belong to the struct?
    available_sender: Sender<T>,
    available_receiver: Option<Receiver<T>>, // FIXME: probably shouldn't belong to the struct?
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

// FIXME: how to return to pool?
impl<T: Send + std::fmt::Debug + 'static> ConnectionPool<T> {
    pub async fn checkout(&mut self) -> impl futures::Future {
        println!("checkout >>> ");
        let (tx, rx) = oneshot::channel::<T>();

        use futures::sink::SinkExt;
        self.queue_in.send(tx).await.unwrap();
        rx
    }

    fn build_connection(&mut self) -> () {
        self.built += 1;
        let mut cx = (self.connection_builder)();

        use futures::sink::SinkExt;

        println!("<<< connection built {:?}", cx);
        // FIXME: should block?
        async_std::task::block_on(async { self.available_sender.send(cx).await.unwrap() });
    }

    // FIXME: no async_std?
    fn spawn_server_loop(&mut self) -> () {
        use async_std::task;
        use futures::prelude::*;

        let mut queue_out = self.queue_out.take().unwrap();
        let mut available_connections = self.available_receiver.take().unwrap();

        task::spawn(async move {
            while let Some(tx) = queue_out.next().await {
                // FIXME: timeout?
                let cx = available_connections.next().await.unwrap();
                println!("sending connection to tx={:?}; cx={:?}", tx, cx);
                tx.send(cx).unwrap();
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
        let mut pool: ConnectionPool<Vec<u8>> = ConnectionPool {
            size: 10,
            built: 0,
            connection_builder: Box::new(|| vec![1u8]),
            connections: Vec::new(),
            queue_in: tx,
            queue_out: Some(rx),
            available_sender,
            available_receiver: Some(available_reciever),
        };

        pool.build_connection();
        pool.build_connection();
        pool.build_connection();
        pool.build_connection();
        pool.build_connection();
        pool.spawn_server_loop();

        use futures::future::FutureExt;
        let c = async_std::task::block_on(async { pool.checkout().map(|i| i).await });

        std::thread::sleep(std::time::Duration::from_secs(2));
        // println!("{:?}", c);
    }
}
