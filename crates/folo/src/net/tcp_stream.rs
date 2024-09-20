use std::{
    cell::{Cell, RefCell}, collections::HashMap, future, net::SocketAddr, rc::Rc, task::{Poll, Waker}, time::Duration
};

use mio::{event::Event, Events, Poll as MioPoll, Token};

type StreamId = u32;

thread_local! {
    // The local mio poll instance bound to current thread.
    static LOCAL_POLL: RefCell<MioPoll> = RefCell::new(MioPoll::new().expect("should never fail"));

    // The local events buffer bound to current thread.
    static EVENTS: RefCell<Events> = RefCell::new(Events::with_capacity(1024));

    // The pending IO events bound to current thread.
    static PENDING_EVENTS: RefCell<HashMap<Token,IoEvent>> = RefCell::new(HashMap::new());

    // The last used token and stream id.
    static LAST_TOKEN: Cell<Token> = Cell::new(Token(0));
    static LAST_STREAM_ID: Cell<u32> = Cell::new(0);
}

pub(crate) fn poll_local_events() -> std::io::Result<()> {
    LOCAL_POLL.with_borrow_mut(|poll| poll_local_events_inner(poll))
}

pub struct TcpStream {
    inner: Rc<TcpStreamInner>,
}

impl TcpStream {
    pub async fn connect(addr: impl Into<SocketAddr>) -> std::io::Result<Self> {
        // Generate the identifiers for the stream
        let token = next_token();
        let id = next_stream_id();
        let mut io = mio::net::TcpStream::connect(addr.into())?;

        // Register the stream with the poll so we can react to an connection event
        LOCAL_POLL.with_borrow_mut(|poll| {
            poll.registry().register(
                &mut io,
                token,
                mio::Interest::READABLE | mio::Interest::WRITABLE,
            )
        })?;

        let stream = Rc::new(TcpStreamInner {
            id,
            io,
            connected: RefCell::new(None),
        });

        let clone: Rc<TcpStreamInner> = Rc::clone(&stream);

        future::poll_fn(|context| {
            try_register_event(
                token,
                IoEvent::Connect(Rc::clone(&clone), context.waker().clone()),
            );

            let mut connected = clone.connected.borrow_mut();

            if let Some(connected) = connected.take() {
                Poll::Ready(connected)
            } else {
                Poll::Pending
            }
        })
        .await?;

        Ok(TcpStream { inner: stream })
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.inner
            .io
            .peer_addr()
            .expect("stream is connected at this point")
    }
}

enum IoEvent {
    Connect(Rc<TcpStreamInner>, Waker),
}

struct TcpStreamInner {
    id: StreamId,
    io: mio::net::TcpStream,
    connected: RefCell<Option<Result<SocketAddr, std::io::Error>>>,
}

fn try_register_event(token: Token, ev: IoEvent) {
    PENDING_EVENTS.with_borrow_mut(|events| {
        events.entry(token).or_insert(ev);
    });
}

fn poll_local_events_inner(poll: &mut MioPoll) -> std::io::Result<()> {
    EVENTS.with_borrow_mut(|events| {
        poll.poll(events, Some(Duration::ZERO))?;
        events.iter().for_each(|ev| handle_event(ev));

        Ok(())
    })
}

fn handle_event(e: &Event) {
    PENDING_EVENTS.with_borrow_mut(|events| {
        if let Some(io) = events.remove(&e.token()) {
            match io {
                IoEvent::Connect(stream, waker) => {
                    *stream.connected.borrow_mut() = Some(stream.io.peer_addr());
                    waker.wake();
                }
            }
        }
    })
}

fn next_token() -> Token {
    let token = Token(LAST_TOKEN.get().0.wrapping_add(1));
    LAST_TOKEN.set(token);
    token
}

fn next_stream_id() -> StreamId {
    let id = LAST_STREAM_ID.get().wrapping_add(1);
    LAST_STREAM_ID.set(id);
    id
}
