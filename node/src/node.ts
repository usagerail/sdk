// Parts only used in long-running Node processes: saving the key snapshot to disk and reporting before exit. Runtimes like Workers never import this file.
import { createHash, randomBytes } from "node:crypto";
import { closeSync, constants, fstatSync, lstatSync, mkdirSync, openSync, readFileSync } from "node:fs";
import { open, rename, unlink } from "node:fs/promises";
import type { ServerResponse } from "node:http";
import { tmpdir } from "node:os";
import path from "node:path";

import type { Engine } from "./engine.ts";
import type { SnapshotStore, SyncSnapshot } from "./key-store.ts";

// Windows has no O_NOFOLLOW
const NO_FOLLOW = constants.O_NOFOLLOW ?? 0;

// The snapshot is written to the local temp directory with mode 600. The temp directory is shared by every user on the machine, and others can put files and symlinks in it;
// reading a snapshot someone else planted would mean trusting their key hashes. So:
// - files live in a subdirectory only the current user can enter (0700), checked before every use: a real directory, owned by us, no access for anyone else;
// - read: open without following symlinks, check owner and mode on that same file descriptor, then read from it, so nothing can be swapped in between;
// - write: create a randomly named file exclusively (O_EXCL), then rename it over the old one, so a process killed mid-write never leaves an unreadable snapshot.
// File names tell apps apart by a hash of the appKey; the appKey itself never appears in a file name
export function fileSnapshotStore(appKey: string, directory = defaultSnapshotDirectory()): SnapshotStore {
  // The usual call is fileSnapshotStore(process.env.USAGERAIL_APP_KEY): when the variable isn't set, give the same message as the engine here instead of letting createHash throw an obscure type error
  if (!appKey) throw new Error("UsageRail: appKey is required (USAGERAIL_APP_KEY from the console)");
  const file =path.join(directory, `snapshot-${createHash("sha256").update(appKey).digest("hex").slice(0, 16)}.json`);
  return {
    load() {
      try {
        assertPrivateDirectory(directory);
      } catch (error) {
        if ((error as { code?: string }).code === "ENOENT") return null;
        throw error;
      }
      let descriptor: number;
      try {
        descriptor = openSync(file, constants.O_RDONLY | NO_FOLLOW);
      } catch (error) {
        if ((error as { code?: string }).code === "ENOENT") return null;
        throw error;
      }
      try {
        const stats = fstatSync(descriptor);
        if (!stats.isFile() || !isPrivate(stats)) throw new Error(`${file} is not a private file owned by this user; ignoring it`);
        const snapshot = JSON.parse(readFileSync(descriptor, "utf8")) as SyncSnapshot;
        return isSnapshot(snapshot) ? snapshot : null;
      } finally {
        closeSync(descriptor);
      }
    },
    async save(snapshot) {
      mkdirSync(directory, { recursive: true, mode: 0o700 });
      assertPrivateDirectory(directory);
      const temporary = path.join(directory, `.snapshot-${randomBytes(8).toString("hex")}.tmp`);
      const handle = await open(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | NO_FOLLOW, 0o600);
      try {
        await handle.writeFile(JSON.stringify(snapshot));
      } finally {
        await handle.close();
      }
      try {
        await rename(temporary, file);
      } catch (error) {
        await unlink(temporary).catch(() => {});
        throw error;
      }
    },
  };
}

function defaultSnapshotDirectory() {
  return path.join(tmpdir(), `usagerail-${typeof process.getuid === "function" ? process.getuid() : "snapshots"}`);
}

function assertPrivateDirectory(directory: string) {
  const stats = lstatSync(directory);
  if (!stats.isDirectory() || !isPrivate(stats)) throw new Error(`${directory} must be a directory that only this user can access`);
}

// Owned by the current user, with no permissions for group or others. Windows has no uid or Unix mode bits, so it isn't checked
function isPrivate(stats: { uid: number; mode: number }) {
  if (typeof process.getuid !== "function") return true;
  return stats.uid === process.getuid() && (stats.mode & 0o077) === 0;
}

function isSnapshot(value: unknown): value is SyncSnapshot {
  const snapshot = value as Partial<SyncSnapshot> | null;
  return Boolean(snapshot && typeof snapshot.saved_at === "string" && Array.isArray(snapshot.keys) && Array.isArray(snapshot.consumers));
}

// Tries to report remaining usage before the process exits (on beforeExit and SIGTERM). Returns a function that undoes this.
// - If the app doesn't listen for SIGTERM itself: report (up to 5 seconds), then exit the way SIGTERM would by default.
// - If it does: the app owns shutdown. Requests will still finish while it drains them, and closing now would lose that usage.
//   So switch to reporting once a second instead; the app calls engine.close() at the end of its shutdown (beforeExit also closes if the process exits naturally).
// Whether the app listens can't be counted when the signal arrives: a once listener registered earlier has already removed itself by the time ours runs.
// So count the existing ones at registration, and note anyone who adds a SIGTERM listener afterwards
export function flushOnExit(engine: Pick<Engine, "close" | "drain">) {
  let appOwnsShutdown = process.listenerCount("SIGTERM") > 0;
  const onNewListener = (event: string | symbol) => {
    if (event === "SIGTERM") appOwnsShutdown = true;
  };
  const onBeforeExit = () => void engine.close();
  const onSigterm = () => {
    if (appOwnsShutdown) {
      engine.drain();
      return;
    }
    void engine.close().finally(() => {
      process.removeListener("beforeExit", onBeforeExit);
      process.kill(process.pid, "SIGTERM");
    });
  };
  process.once("beforeExit", onBeforeExit);
  process.once("SIGTERM", onSigterm);
  // Start watching only after registering our own, so we don't count ourselves
  process.on("newListener", onNewListener);
  return () => {
    process.removeListener("newListener", onNewListener);
    process.removeListener("beforeExit", onBeforeExit);
    process.removeListener("SIGTERM", onSigterm);
  };
}

// An SSE response counts once, as soon as it starts sending: it's one long-lived connection. Express and Fastify both sit on Node's ServerResponse and share this. It can't wait until the connection ends: a stream can stay open for hours,
// leaving the request unmetered all that time, and past 24 hours the server no longer accepts data for that minute.
// Response headers always go out through writeHead (res.write and flushHeaders end up there too), so check for event-stream there
export function meterEventStreamsAtStart(res: ServerResponse, onStreamStart: () => void) {
  const writeHead = res.writeHead;
  res.writeHead = function (this: ServerResponse, ...args: unknown[]) {
    const result = (writeHead as (...parameters: unknown[]) => ServerResponse).apply(this, args);
    if (res.statusCode < 300 && contentTypeOf(res, args).startsWith("text/event-stream")) onStreamStart();
    return result;
  } as ServerResponse["writeHead"];
}

// Headers passed straight to writeHead(status, headers) don't show up in getHeader, so look in the arguments; they can be an object or a raw array
function contentTypeOf(res: ServerResponse, args: unknown[]) {
  for (const arg of args) {
    if (Array.isArray(arg)) {
      const pairs = arg.every(Array.isArray) ? (arg as unknown[][]) : chunk(arg);
      const found = pairs.find(([name]) => String(name).toLowerCase() === "content-type");
      if (found) return String(found[1]).toLowerCase();
    } else if (arg && typeof arg === "object") {
      const found = Object.entries(arg).find(([name]) => name.toLowerCase() === "content-type");
      if (found) return String(found[1]).toLowerCase();
    }
  }
  return String(res.getHeader("content-type") ?? "").toLowerCase();
}

function chunk(flat: unknown[]) {
  const pairs: unknown[][] = [];
  for (let index = 0; index + 1 < flat.length; index += 2) pairs.push([flat[index], flat[index + 1]]);
  return pairs;
}
