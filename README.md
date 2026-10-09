# Custom Network Stack

A Python-based networking project that implements client-server communication, file downloads, and AI integration using Google Gemini.

The project demonstrates several networking concepts, including TCP, Reliable UDP, DNS, DHCP, and proxy servers.

## Features
- TCP client-server communication
- Reliable UDP with acknowledgments and retransmissions
- File downloads from URLs
- Google Gemini API integration
- DNS and DHCP server implementations
- Proxy server with caching
- Multithreading support

## Running the Project

Install the required libraries:

```bash
pip install google-genai dnspython
```

Set your Gemini API key using the `GEMINI_API_KEY` environment variable.

Run the server:

```bash
python server.py
```

Run the client in a separate terminal:

```bash
python client.py
```
