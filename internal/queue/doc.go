// Package queue owns raw body admission, canonical payload identity, and
// durable command receipts under receipts.md ??1?5. The HTTP adapter supplies
// an authenticated caller identity and route. Command validation and thread
// execution are the command handler's responsibility.

package queue
