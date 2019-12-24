package pubsub

import (
	"testing"
)

/*
subscribe->publish->poll->unsubscribe
*/
func TestSimpleSubscription(t *testing.T) {
	ps := NewPubSub()
	if ps.Subscribe("topic", "subscriber") != nil {
		t.Error("Subscribe() failed")
	}
	if ps.Publish("topic", "message") != nil {
		t.Error("Publish() failed")
	}
	m, e := ps.Poll("topic", "subscriber")
	if e != nil {
		t.Error("Poll() failed")
	}
	if *m != "message" {
		t.Error("Poll() returned bad message")
	}
	_, e = ps.Poll("topic", "subscriber")
	if e == nil {
		t.Error("Poll() returned unexpected messages")
	}
	if ps.Unsubscribe("topic", "subscriber") != nil {
		t.Error("Unsubscribe() failed")
	}
}

/*
2 subscribers, 2 topics, each subscriber subscribed to one of topics,
publish 1 message per topic and make sure that each subsriber receives relevant message
*/
func TestSubscriberRecievesOnlyRelevantMessages(t *testing.T) {
	ps := NewPubSub()
	if ps.Subscribe("t1", "s1") != nil {
		t.Error("Subscribe(t1, s1) failed")
	}
	if ps.Subscribe("t2", "s2") != nil {
		t.Error("Subscribe(t2, s2) failed")
	}
	if ps.Publish("t1", "m1") != nil {
		t.Error("Publish(t1, m1) failed")
	}
	if ps.Publish("t2", "m2") != nil {
		t.Error("Publish(t2, m2) failed")
	}
	m1, e := ps.Poll("t1", "s1")
	if e != nil {
		t.Error("Poll() failed")
	} else if *m1 != "m1" {
		t.Error("Poll() returned bad message")
	}
	_, e = ps.Poll("t1", "s2")
	if e == nil {
		t.Error("Poll(s1) returned unexpected messages")
	}
	m2, e := ps.Poll("t2", "s2")
	if e != nil {
		t.Error("Poll(t2, s2) failed")
	} else if *m2 != "m2" {
		t.Error("Poll(m2) returned bad message")
	}
	_, e = ps.Poll("t1", "s2")
	if e == nil {
		t.Error("Poll(t1, s2) returned unexpected messages")
	}
	if ps.Unsubscribe("t1", "s1") != nil {
		t.Error("Unsubscribe(t1, s1) failed")
	}
	if ps.Unsubscribe("t2", "s2") != nil {
		t.Error("Unsubscribe(t2, s2) failed")
	}
}

/*
s1 is subsribed to t1, m1 is published to t1, s2 is subsribed to t1, m2 is published to t1.
s1 should receive both messages, s2 should receive only m2.
*/
func TestSubscribingDoesntDeliverPreviouslyPublishedMessages(t *testing.T) {
	ps := NewPubSub()
	if ps.Subscribe("t1", "s1") != nil {
		t.Error("Subscribe(t1, s1) failed")
	}
	if ps.Publish("t1", "m1") != nil {
		t.Error("Publish(t1, m1) failed")
	}
	if ps.Subscribe("t1", "s2") != nil {
		t.Error("Subscribe(t1, s2) failed")
	}
	if ps.Publish("t1", "m2") != nil {
		t.Error("Publish(t1, m2) failed")
	}
	{
		m1, e := ps.Poll("t1", "s1")
		if e != nil {
			t.Error("Poll() failed")
		} else if *m1 != "m1" {
			t.Error("Poll() returned bad message")
		}
		m2, e := ps.Poll("t1", "s1")
		if e != nil {
			t.Error("Poll() failed")
		} else if *m2 != "m2" {
			t.Error("Poll() returned bad message")
		}
		_, e = ps.Poll("t1", "s1")
		if e == nil {
			t.Error("Poll(s1) returned unexpected messages")
		}
	}
	{
		m2, e := ps.Poll("t1", "s2")
		if e != nil {
			t.Error("Poll(t1, s2) failed")
		} else if *m2 != "m2" {
			t.Error("Poll(m2) returned bad message")
		}
		_, e = ps.Poll("t1", "s2")
		if e == nil {
			t.Error("Poll(t1, s2) returned unexpected messages")
		}
	}
	if ps.Unsubscribe("t1", "s1") != nil {
		t.Error("Unsubscribe(t1, s1) failed")
	}
	if ps.Unsubscribe("t1", "s2") != nil {
		t.Error("Unsubscribe(t1, s2) failed")
	}
}

/*
Test unsubsription from non existing subscriber, topic.
Test polling without subscription.
etc
*/
func TestNegative(t *testing.T) {

}

func TestHighLoad(t *testing.T) {

}
