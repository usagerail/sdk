// Two small helpers shared by everything built on the Web standard Response (Hono, Next.js route handlers, Workers).

// An SSE response (text/event-stream) counts once, when it starts sending: it is one long-lived connection
export function isEventStream(response: Response) {
  return response.status < 300 && (response.headers.get("content-type") ?? "").toLowerCase().startsWith("text/event-stream");
}

// Wraps a response body so onEnd runs exactly once, when the stream is fully read or cancelled (the client disconnected)
export function whenStreamEnds(body: ReadableStream<Uint8Array>, onEnd: () => void) {
  const reader = body.getReader();
  let ended = false;
  // Cancel in progress: settle only after the source's own cancel() has run, so track() calls made inside it still count (e.g. recording the tokens already generated when the client drops)
  let cancelling = false;
  const finish = () => {
    if (ended) return;
    ended = true;
    onEnd();
  };
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { done, value } = await reader.read();
        // On cancel, the read still pending comes back as "done" before the source's cancel() runs: don't settle here, leave it to cancel below
        if (cancelling) return;
        if (done) {
          finish();
          controller.close();
        } else {
          controller.enqueue(value);
        }
      } catch (error) {
        if (cancelling) return;
        finish();
        controller.error(error);
      }
    },
    async cancel(reason) {
      cancelling = true;
      try {
        await reader.cancel(reason);
      } finally {
        finish();
      }
    },
  });
}
