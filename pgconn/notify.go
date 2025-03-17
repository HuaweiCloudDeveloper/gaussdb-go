package pgconn

// Package pq is a pure Go Postgres driver for the database/sql package.
// This module contains support for Postgres LISTEN/NOTIFY.

// Notification represents a single notification from the database.
type NotificationP struct {
	// Process ID (PID) of the notifying postgres backend.
	BePid int
	// Name of the channel the notification was sent on.
	Channel string
	// Payload, or the empty string if unspecified.
	Extra string
}

func recvNotification(r *readBuf) *NotificationP {
	bePid := r.int32()
	channel := r.string()
	extra := r.string()

	return &NotificationP{bePid, channel, extra}
}
