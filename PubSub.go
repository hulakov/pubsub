package pubsub

import (
	"errors"
	"fmt"
)

/*

General idea:
 - message chain is implemented as single linked list
 - message chain always contains trailing fake message (i.e. contains no text)
 - topic always point to the trailing fake message
 - subscriber is implemented as list of subscriptions
 - subscription points to a message in message chain
 - in case if topic doesn't contain any relevant messages (or empty) subscription points to trailing fake message

Message chain clean-up is performed automatically by GC after message referenced neither by topic nor subscription.
The implementation isn't thread-safe!
Despite requirements some calls may return error in case of undefinded behaviour (like subsribe particular
subscriber to particular topic for the second time)

 - What is the message publish algorithm complexity in big-O notation?
   O(n)=1 - search in hashmap and pushing item to list

 - What is the message poll algorithm complexity in big-O notation?
   O(n)=1 - 2 search operations in hashmap and advancing of next message

 - What is the memory (space) complexity in big-O notation for the algorithm?
   O(n)=n which when n is total number of messages, subscriptions, subscribers and topics.
*/

type message struct {
	jsonBody *string
	next     *message
}

type subscription struct {
	head *message
}

type subscriber struct {
	subscriptions map[string]*subscription
}

type topic struct {
	tail *message
}

type PubSub struct {
	subscribers map[string]*subscriber
	topics      map[string]*topic
}

func NewPubSub() PubSub {
	return PubSub{subscribers: make(map[string]*subscriber), topics: make(map[string]*topic)}
}

func (pubsub *PubSub) Publish(topicName, jsonBody string) error {
	t, ok := pubsub.topics[topicName]
	if !ok {
		// topic doesn't exists -> no subscribers -> nobody will receive a message
		return nil
	}

	// push message to topic and advance tail pointer
	t.tail.jsonBody = &jsonBody
	t.tail.next = &message{}
	t.tail = t.tail.next

	return nil
}

func (pubsub *PubSub) Subscribe(topicName, subscriberName string) error {
	// get trailing empty message from topic (if no such topic it'll be created)
	var m *message
	t, ok := pubsub.topics[topicName]
	if ok {
		m = t.tail
	} else {
		m = &message{}
		pubsub.topics[topicName] = &topic{tail: m}
	}

	// get or create subscriber
	s, ok := pubsub.subscribers[subscriberName]
	if !ok {
		s = &subscriber{subscriptions: make(map[string]*subscription)}
		pubsub.subscribers[subscriberName] = s
	}

	// add new subscription
	_, ok = s.subscriptions[topicName]
	if ok {
		return errors.New(fmt.Sprintf("%s is already subsribed to %s", topicName, subscriberName))
	}
	s.subscriptions[topicName] = &subscription{head: m}

	return nil
}

func (pubsub *PubSub) Unsubscribe(topicName, subscriberName string) error {
	// get subscriber
	s, ok := pubsub.subscribers[subscriberName]
	if !ok {
		return errors.New(fmt.Sprintf("The is no subsriber %s", subscriberName))
	}

	// get subscription
	_, ok = s.subscriptions[topicName]
	if !ok {
		return errors.New(fmt.Sprintf("%s isn't subsribed to %s", topicName, subscriberName))
	}
	delete(s.subscriptions, topicName)

	// remove subscribers without subscrioptions
	if len(s.subscriptions) == 0 {
		delete(pubsub.subscribers, subscriberName)
	}

	// TODO remove topic if no one subscribed to it

	return nil
}

func (pubsub *PubSub) Poll(topicName, subscriberName string) (*string, error) {
	// get subscriber
	s, ok := pubsub.subscribers[subscriberName]
	if !ok {
		return nil, errors.New(fmt.Sprintf("subscriber %s isn't registered", subscriberName))
	}

	// get subscription
	ss, ok := s.subscriptions[topicName]
	if !ok {
		return nil, errors.New(fmt.Sprintf("%s isn't subsribed to %s", topicName, subscriberName))
	}

	// check if any relevant messages available
	if ss.head.next == nil {
		return nil, errors.New(fmt.Sprintf("topic %s is empty", topicName))
	}

	// get message and advance message chain pointer
	result := ss.head.jsonBody
	ss.head = ss.head.next

	return result, nil
}
