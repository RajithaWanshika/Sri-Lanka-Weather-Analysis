#!/usr/bin/env python3

import http.server
import socketserver
import os
import sys

PORT = 8000


class MyHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        super().end_headers()

    def log_message(self, format, *args):
        sys.stderr.write(
            "%s - - [%s] %s\n"
            % (self.address_string(), self.log_date_time_string(), format % args)
        )


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    if not os.path.exists("results/dashboard_data.json"):
        print("Warning: results/dashboard_data.json not found!")
        print("Please run: python3 process_results_data.py")
        print()

    with socketserver.TCPServer(("", PORT), MyHTTPRequestHandler) as httpd:
        print(f"Dashboard server running at http://localhost:{PORT}/")
        print(f"Open in browser: http://localhost:{PORT}/dashboard.html")
        print(f"Serving files from {os.getcwd()}")
        print("\nPress Ctrl+C to stop the server")

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n\nServer stopped.")
