import threading
from collections import defaultdict

from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit, join_room, leave_room, rooms

from kafka import KafkaConsumer

app = Flask(__name__)

socketio = SocketIO(app)

topics_threads = {}
ids_per_topic = defaultdict(set)

class SocketIOKafkaConsumer(threading.Thread):
    def __init__(self, topic):
        threading.Thread.__init__(self)
        self.topic = topic
        self.end = False
        self.consumer = None
        app.logger.debug("New topic consumer {} created".format(self.topic))

    def run(self):
        count = 0
        self.c = KafkaConsumer(self.topic)

        while not self.end:
            msg = next(self.c)
            socketio.emit('new_message',
                          {
                              'topic': msg.topic,
                              'key': msg.key,
                              'data': msg.value
                          },
                          namespace='/kafka-tail',
                          room=self.topic)
        app.logger.debug("Thread for topic {} terminated".format(self.topic))

def leave_topic_clean(topic, sid):
    ids_per_topic[topic].discard(sid)
    if topic in topics_threads and len(ids_per_topic[topic]) == 0:
        topics_threads[topic].end = True
        del topics_threads[topic]

@app.route('/multi')
def mutli():
    return render_template('multiindex.html')

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('join', namespace='/kafka-tail')
def on_join(topic):
    app.logger.debug("Trying to join kafka topic {}".format(topic))
    join_room(topic)

    if topic not in topics_threads:
        t = SocketIOKafkaConsumer(topic)
        topics_threads[topic] = t
        t.start()

    ids_per_topic[topic].add(request.sid)

    emit({"ok": "topic {} joined".format(topic)})

@socketio.on('leave', namespace='/kafka-tail')
def on_leave(topic):
    leave_room(topic)
    leave_topic_clean(topic, request.sid)

@socketio.on('disconnect', namespace='/kafka-tail')
def disconnect():
    app.logger.debug("Sid {} disconnected".format(request.sid))
    for topic, ids in ids_per_topic.items():
        if request.sid in ids:
            leave_topic_clean(topic, request.sid)

if __name__ == '__main__':
    socketio.run(app)
