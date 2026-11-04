"""Black-box checks for the Lab 1 long-polling, JSON broker implementation."""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
import http.client
import json
from pathlib import Path
import socket
import threading
import time
import unittest
from urllib.parse import urlencode, urlsplit
import uuid


@dataclass
class Response:
    status: int
    body: bytes
    content_type: str


class Client:
    def __init__(self, base_url, timeout):
        url = urlsplit(base_url)
        if url.scheme != "http" or not url.hostname or url.path not in ("", "/"):
            raise ValueError("Use an HTTP origin, for example http://localhost:8080")
        if url.query or url.fragment or url.username or url.password:
            raise ValueError("URL must not contain credentials, query or fragment")
        self.host = url.hostname
        self.port = url.port if url.port is not None else 80
        if self.port == 0:
            raise ValueError("Port must be between 1 and 65535")
        self.timeout = timeout

    def request(self, method, path, body=None, timeout=None):
        conn = http.client.HTTPConnection(
            self.host, self.port, timeout=timeout or self.timeout
        )
        try:
            conn.request(method, path, body=body)
            response = conn.getresponse()
            data = response.read()
            length = response.getheader("Content-Length")
            if length is not None and method != "HEAD" and response.status != 204:
                if len(data) != int(length):
                    raise AssertionError("Content-Length does not match response body")
            return Response(response.status, data, response.getheader("Content-Type", ""))
        finally:
            conn.close()

    def connect(self):
        return socket.create_connection((self.host, self.port), timeout=self.timeout)

    def raw(self, data):
        with self.connect() as conn:
            conn.sendall(data)
            conn.shutdown(socket.SHUT_WR)
            response = http.client.HTTPResponse(conn)
            response.begin()
            return Response(
                response.status, response.read(), response.getheader("Content-Type", "")
            )


class Lab1Tests(unittest.TestCase):
    """Each case uses private file paths and topics on one shared server."""

    def setUp(self):
        self.prefix = f"{self.run_prefix}/{self._testMethodName}"
        self.subscriptions = set()
        self.subscription_lock = threading.Lock()
        self.addCleanup(self.cleanup_subscriptions)

    def cleanup_subscriptions(self):
        for topic, subscriber_id in list(self.subscriptions):
            response = self.client.request(
                "POST", "/unsubscribe?" + urlencode({"topic": topic, "id": subscriber_id})
            )
            self.assertIn(response.status, (200, 400), "subscription cleanup failed")

    def topic(self, name="news"):
        return f"{self.prefix}/{name}"

    def subscribe(self, topic):
        response = self.client.request("POST", "/subscribe?" + urlencode({"topic": topic}))
        self.assertEqual(response.status, 200)
        self.assertEqual(response.content_type.split(";")[0], "application/json")
        subscriber_id = json.loads(response.body)["id"]
        self.assertIs(type(subscriber_id), int)
        self.assertGreaterEqual(subscriber_id, 0)
        with self.subscription_lock:
            self.assertNotIn((topic, subscriber_id), self.subscriptions)
            self.subscriptions.add((topic, subscriber_id))
        return subscriber_id

    def unsubscribe(self, topic, subscriber_id):
        path = "/unsubscribe?" + urlencode({"topic": topic, "id": subscriber_id})
        self.assertEqual(self.client.request("POST", path).status, 200)
        with self.subscription_lock:
            self.subscriptions.discard((topic, subscriber_id))

    def publish(self, topic, body):
        response = self.client.request("POST", "/publish?" + urlencode({"topic": topic}), body)
        self.assertEqual(response.status, 200)

    def events(self, topic, subscriber_id, timeout=None):
        path = "/events?" + urlencode({"topic": topic, "id": subscriber_id})
        return self.client.request("GET", path, timeout=timeout)

    def message(self, response):
        self.assertEqual(response.status, 200)
        self.assertEqual(response.content_type.split(";")[0], "application/json")
        message = json.loads(response.body)
        self.assertIs(type(message["Seq"]), int)
        self.assertGreaterEqual(message["Seq"], 0)
        return message["Seq"], base64.b64decode(message["Data"], validate=True)

    def test_01_file_types_and_round_trip(self):
        types = {"html": "text/html", "txt": "text/plain", "gif": "image/gif",
                 "jpeg": "image/jpeg", "jpg": "image/jpeg", "css": "text/css",
                 "JPG": "image/jpeg"}
        for extension, mime in types.items():
            with self.subTest(extension=extension):
                path = f"/{self.prefix}/nested/file.{extension}"
                data = "文本 content".encode() if mime.startswith("text/") else bytes(range(256))
                self.assertEqual(self.client.request("POST", path, data).status, 200)
                response = self.client.request("GET", path)
                self.assertEqual((response.status, response.body), (200, data))
                self.assertEqual(response.content_type.split(";")[0], mime)
        path = f"/{self.prefix}/empty.txt"
        self.assertEqual(self.client.request("POST", path, b"").status, 200)
        self.assertEqual(self.client.request("GET", path).body, b"")

    def test_02_file_errors_and_methods(self):
        self.assertEqual(self.client.request("GET", f"/{self.prefix}/missing.txt").status, 404)
        for method in ("GET", "POST"):
            for name in ("file.png", "file", "file.json"):
                with self.subTest(method=method, name=name):
                    self.assertEqual(self.client.request(method, f"/{self.prefix}/{name}").status, 400)
        for method in ("HEAD", "PUT", "DELETE", "OPTIONS", "PATCH", "TRACE", "CUSTOM"):
            for extension in ("txt", "png"):
                with self.subTest(method=method, extension=extension):
                    self.assertEqual(self.client.request(method, f"/{self.prefix}/file.{extension}").status, 501)
        parent = f"/{self.prefix}/parent.txt"
        self.assertEqual(self.client.request("POST", parent, b"file").status, 200)
        self.assertEqual(self.client.request("POST", parent + "/child.txt", b"x").status, 400)
        self.assertEqual(self.client.request("GET", f"/{self.prefix}/nested.txt").status, 404)
        self.assertEqual(self.client.request("POST", f"/{self.prefix}/nested.txt/file.txt", b"x").status, 200)
        self.assertEqual(self.client.request("GET", f"/{self.prefix}/nested.txt").status, 400)

    def test_03_malformed_requests(self):
        for data in (b"invalid\r\n\r\n", b"GET /a.txt HTTP/1.1\r\nBroken Header\r\n\r\n",
                     b"POST /a.txt HTTP/1.1\r\nContent-Length: nope\r\n\r\n"):
            with self.subTest(request=data):
                self.assertEqual(self.client.raw(data).status, 400)

    def test_04_atomic_file_updates(self):
        path = f"/{self.prefix}/atomic.txt"
        versions = [bytes([65 + i]) * (64 * 1024) for i in range(3)]
        self.assertEqual(self.client.request("POST", path, versions[0]).status, 200)

        def worker(index):
            for _ in range(20):
                if index < 3:
                    self.assertEqual(self.client.request("POST", path, versions[index]).status, 200)
                else:
                    response = self.client.request("GET", path)
                    self.assertEqual(response.status, 200)
                    self.assertIn(response.body, versions, "partial or mixed file version")

        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(worker, range(6)))

    def test_05_truncated_upload(self):
        path = f"/{self.prefix}/original.txt"
        self.assertEqual(self.client.request("POST", path, b"original").status, 200)
        raw = f"POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\nshort".encode()
        self.assertEqual(self.client.raw(raw).status, 400)
        self.assertEqual(self.client.request("GET", path).body, b"original")
        raw = b"POST /publish?topic=unused HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\nshort"
        self.assertEqual(self.client.raw(raw).status, 400)

    def test_06_broker_parameters_and_methods(self):
        for route in ("subscribe", "unsubscribe", "publish", "events"):
            method = "GET" if route == "events" else "POST"
            for query in ("", "?topic="):
                with self.subTest(route=route, query=query):
                    self.assertEqual(self.client.request(method, "/" + route + query).status, 400)
            for wrong in ("POST", "GET", "DELETE", "PUT"):
                if wrong != method:
                    self.assertEqual(self.client.request(wrong, "/" + route + "?topic=x&id=0").status, 501)
        topic = self.topic()
        subscriber_id = self.subscribe(topic)
        for route, method in (("events", "GET"), ("unsubscribe", "POST")):
            for value in (None, "", "abc", "-1", "9999999999999999999999999"):
                query = {"topic": topic}
                if value is not None:
                    query["id"] = value
                with self.subTest(route=route, id=value):
                    self.assertEqual(self.client.request(method, "/" + route + "?" + urlencode(query)).status, 400)
        self.assertEqual(self.events(self.topic("wrong"), subscriber_id).status, 400)
        self.unsubscribe(topic, subscriber_id)
        self.assertEqual(self.events(topic, subscriber_id).status, 400)
        path = "/unsubscribe?" + urlencode({"topic": topic, "id": subscriber_id})
        self.assertEqual(self.client.request("POST", path).status, 400)

    def test_07_broadcast_isolation_and_membership(self):
        topic, other = self.topic(), self.topic("other")
        self.publish(topic, b"before subscription")
        first, second = self.subscribe(topic), self.subscribe(topic)
        outsider = self.subscribe(other)
        self.publish(topic, b"\x00\xffbroadcast")
        a, b = self.message(self.events(topic, first)), self.message(self.events(topic, second))
        self.assertEqual(a, b)
        self.assertEqual(a[1], b"\x00\xffbroadcast")
        self.publish(other, b"isolated")
        self.assertEqual(self.message(self.events(other, outsider))[1], b"isolated")
        late = self.subscribe(topic)
        self.publish(topic, b"after late subscription")
        self.assertEqual(self.message(self.events(topic, late))[1], b"after late subscription")
        self.unsubscribe(topic, first)
        self.publish(topic, b"after unsubscribe")
        self.assertEqual(self.events(topic, first).status, 400)
        self.assertEqual(self.message(self.events(topic, second))[1], b"after late subscription")
        self.assertEqual(self.message(self.events(topic, second))[1], b"after unsubscribe")

    def test_08_concurrent_publish_order(self):
        topic = self.topic()
        subscribers = [self.subscribe(topic) for _ in range(2)]
        bodies = [f"publisher-message-{i}".encode() for i in range(80)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda body: self.publish(topic, body), bodies))
        deliveries = [[self.message(self.events(topic, sid)) for _ in bodies] for sid in subscribers]
        self.assertEqual(deliveries[0], deliveries[1], "subscribers saw different publication orders")
        self.assertEqual({data for _, data in deliveries[0]}, set(bodies))
        sequences = [seq for seq, _ in deliveries[0]]
        self.assertTrue(all(a < b for a, b in zip(sequences, sequences[1:])))

    def test_09_bounded_queue_does_not_block_publish(self):
        topic = self.topic()
        subscriber_id = self.subscribe(topic)
        for i in range(self.queue_capacity + 10):
            self.publish(topic, str(i).encode())
        messages = [self.message(self.events(topic, subscriber_id)) for _ in range(self.queue_capacity)]
        # This implementation drops the incoming message when the queue is full.
        self.assertEqual([data for _, data in messages], [str(i).encode() for i in range(self.queue_capacity)])
        self.publish(topic, b"after draining")
        self.assertEqual(self.message(self.events(topic, subscriber_id))[1], b"after draining")

    def test_10_long_poll_delivers_when_message_arrives(self):
        topic = self.topic()
        subscriber_id = self.subscribe(topic)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(self.events, topic, subscriber_id)
            time.sleep(0.2)
            self.assertFalse(pending.done(), "empty Events returned without waiting")
            self.publish(topic, b"wake up")
            self.assertEqual(self.message(pending.result(timeout=self.client.timeout))[1], b"wake up")

    def test_11_long_poll_timeout(self):
        topic = self.topic()
        subscriber_id = self.subscribe(topic)
        start = time.monotonic()
        response = self.events(topic, subscriber_id, timeout=self.poll_timeout + 5)
        elapsed = time.monotonic() - start
        self.assertEqual((response.status, response.body), (204, b""))
        self.assertGreaterEqual(elapsed, max(0, self.poll_timeout - 1))
        self.assertLess(elapsed, self.poll_timeout + 5)

    def test_12_unsubscribe_wakes_waiting_events(self):
        topic = self.topic()
        subscriber_id = self.subscribe(topic)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(self.events, topic, subscriber_id)
            time.sleep(0.2)
            self.assertFalse(pending.done())
            self.unsubscribe(topic, subscriber_id)
            self.assertEqual(pending.result(timeout=self.client.timeout).status, 400)

    def test_13_waiting_polls_leave_publish_capacity(self):
        topic = self.topic()
        subscribers = [self.subscribe(topic) for _ in range(12)]
        with ThreadPoolExecutor(max_workers=12) as pool:
            pending = [pool.submit(self.events, topic, sid) for sid in subscribers]
            time.sleep(0.2)
            self.assertTrue(all(not future.done() for future in pending))
            self.publish(topic, b"broadcast to waiting polls")
            for future in pending:
                self.assertEqual(self.message(future.result(timeout=self.client.timeout))[1], b"broadcast to waiting polls")

    def test_14_concurrent_subscribe_unsubscribe_publish(self):
        topic = self.topic()

        def worker(index):
            for i in range(10):
                subscriber_id = self.subscribe(topic)
                self.publish(topic, f"{index}-{i}".encode())
                self.message(self.events(topic, subscriber_id))
                self.unsubscribe(topic, subscriber_id)
                self.assertEqual(self.events(topic, subscriber_id).status, 400)

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(worker, range(4)))

    def test_15_ten_active_request_limit(self):
        # Requires an otherwise idle server: hold ten incomplete HTTP requests.
        with ExitStack() as stack:
            held = []
            for _ in range(10):
                conn = stack.enter_context(self.client.connect())
                held.append(conn)
                conn.sendall(b"GET /incomplete.txt HTTP/1.1\r\n")
                # A local port proxy may acknowledge connect before forwarding it.
                # Pace setup so the final request cannot overtake these sockets.
                time.sleep(0.03)
            time.sleep(0.15)
            extra = stack.enter_context(self.client.connect())
            path = f"/{self.prefix}/missing.txt"
            extra.sendall(f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
            extra.settimeout(0.3)
            with self.assertRaises(socket.timeout, msg="11th request was processed before a slot was free"):
                extra.recv(1)
            held[0].close()
            extra.settimeout(self.client.timeout)
            response = http.client.HTTPResponse(extra)
            response.begin()
            self.assertEqual(response.status, 404)
            response.read()


def positive_number(value):
    value = float(value)
    if not 0 < value < 3600:
        raise argparse.ArgumentTypeError("must be greater than 0 and less than 3600")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--timeout", type=positive_number, default=5, help="ordinary request timeout in seconds")
    parser.add_argument("--poll-timeout", type=positive_number, default=30, help="expected server long-poll timeout")
    parser.add_argument("--queue-capacity", type=int, default=100, help="configured subscriber queue capacity (at least 80)")
    parser.add_argument("--report", type=Path, help="optional JSON results file")
    args = parser.parse_args()
    if args.queue_capacity < 80:
        parser.error("this suite's 80-message order test requires queue capacity >= 80")
    try:
        Lab1Tests.client = Client(args.base_url, args.timeout)
    except ValueError as error:
        parser.error(str(error))
    Lab1Tests.poll_timeout = args.poll_timeout
    Lab1Tests.queue_capacity = args.queue_capacity
    Lab1Tests.run_prefix = "__lab1_test_" + uuid.uuid4().hex[:12]
    print(f"Target: {args.base_url}; test file prefix: /{Lab1Tests.run_prefix}/", flush=True)
    try:
        with Lab1Tests.client.connect():
            pass
    except OSError as error:
        print(f"Cannot connect to server: {error}")
        return 2
    started = time.monotonic()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(Lab1Tests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    unverified = ["CLI port argument handling", "binary name and compilation", "net-only networking and no frameworks",
                  "Go race detector", "cloud deployment", "architectures other than the target server"]
    report = {
        "base_url": args.base_url, "file_prefix": "/" + Lab1Tests.run_prefix + "/",
        "tests_run": result.testsRun, "successful": result.wasSuccessful(),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "failures": [{"test": str(test), "traceback": detail} for test, detail in result.failures],
        "errors": [{"test": str(test), "traceback": detail} for test, detail in result.errors],
        "unverified": unverified,
    }
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"JSON report: {args.report}")
    print("Test files remain under the printed prefix; subscriptions are cleaned up.")
    print("Not verified by black-box HTTP tests: " + "; ".join(unverified))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
