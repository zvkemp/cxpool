// meta FIXME: this should be more like a GenServer.
// I.e., the ConnectionPool has an internal state, and a mailbox
// handle_cast with a checkout message should reply to a oneshot sender with a future
// that eventually resolves into a connection, either as the result of the connection being built,
// or as the result of another connection being returned to the pool.
//
use futures::channel::mpsc::{self, Receiver, Sender};
use futures::channel::oneshot;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug)]
pub struct Connection<T: 'static + Send + Debug> {
    inner: Option<T>,
    rubberband: Option<Sender<Event<T>>>,
}

impl<T: 'static + Send + std::fmt::Debug> Drop for Connection<T> {
    fn drop(&mut self) {
        let mut rubberband = self.rubberband.take().unwrap();
        let inner = self.inner.take().unwrap();

        async_std::task::spawn(async move {
            println!("returning connection: {:?}", inner);
            rubberband.send(Event::Return(inner)).await.unwrap();
        });
    }
}

impl<T: 'static + Send + std::fmt::Debug> Connection<T> {
    pub fn inner_mut(&mut self) -> Result<&mut T, ()> {
        match self.inner {
            Some(ref mut x) => Ok(x),
            None => Err(()),
        }
    }
}

pub struct ConnectionPool<T: 'static + Send + Debug> {
    size: usize,
    built: usize,
    event_sender: Sender<Event<T>>,
    events: Receiver<Event<T>>,
    connection_builder: Box<dyn Fn() -> T + Send>,
}

#[derive(Debug)]
pub enum Event<T> {
    Wait(oneshot::Sender<T>),
    Return(T),
}

pub struct ConnectionPoolHandle<T: 'static + Send + Debug> {
    event_sender: Sender<Event<T>>,
}

impl<T: 'static + Send + Debug> ConnectionPoolHandle<T> {
    pub async fn checkout(&mut self) -> Connection<T> {
        let (tx, rx) = oneshot::channel::<T>();
        self.event_sender.send(Event::Wait(tx)).await.unwrap();

        use futures::future::FutureExt;

        rx.map(|cx| Connection {
            inner: Some(cx.unwrap()),
            rubberband: Some(self.event_sender.clone()),
        }).await
    }
}

impl<T: 'static + Send + Debug> ConnectionPool<T> {
    pub fn build(builder: Box<dyn Fn() -> T + Send>) -> Self {
        let (event_sender, events) = mpsc::channel(512);
        ConnectionPool {
            size: 10,
            built: 0,
            event_sender,
            events,
            connection_builder: builder,
        }
    }

    pub fn handle(&self) -> ConnectionPoolHandle<T> {
        ConnectionPoolHandle { event_sender: self.event_sender.clone() }
    }

    async fn run(&mut self) -> Result<(), ()> {
        use std::collections::VecDeque;
        let mut available: VecDeque<T> = VecDeque::with_capacity(10);
        let mut waiters: VecDeque<oneshot::Sender<T>> = VecDeque::with_capacity(512);

        while let Some(event) = self.events.next().await {
            match event {
                // FIXME: check health?
                Event::Return(cx) => match waiters.pop_front() {
                    Some(sender) => sender.send(cx).unwrap(),
                    None => available.push_back(cx),
                },
                Event::Wait(sender) => {
                    match available.pop_front() {
                        Some(cx) => sender.send(cx).unwrap(),
                        None => {
                            if self.built < self.size {
                                self.built += 1;
                                let cx = (self.connection_builder)();
                                sender.send(cx).unwrap();
                            } else {
                                waiters.push_back(sender); // FIXME: handle overflow
                            }
                        }
                    }
                }
            }

            println!("built={:?}; available={:?}; waiting={:?}", self.built, available, waiters);
        }

        Ok(())
    }
}

mod tests {
    use super::*;
    #[test]
    fn it_works() {
        let mut pool = ConnectionPool::build(Box::new(|| Vec::<u8>::new()));
        let mut handle = pool.handle();

        async_std::task::spawn(async move { pool.run().await });

        use futures::future::FutureExt;
        {
            let mut c = async_std::task::block_on(async { handle.checkout().await });
            c.inner_mut().map(|x| x.push(2)).unwrap();
        }

        std::thread::sleep(std::time::Duration::from_millis(100));
        let mut d = async_std::task::block_on(async { handle.checkout().await });
        d.inner_mut().map(|x| x.push(3)).unwrap();
        println!("checkout2: {:?}", d);
    }
}
