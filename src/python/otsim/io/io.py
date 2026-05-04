from __future__ import annotations

import json, signal, sys, threading, typing

import otsim.msgbus.envelope as envelope
import xml.etree.ElementTree as ET

from collections import defaultdict

from otsim.helics_helper     import DataType, Endpoint, HelicsFederate, Message, Publication, Subscription
from otsim.msgbus.envelope   import Envelope, MetricKind, Point
from otsim.msgbus.metrics    import MetricsPusher
from otsim.msgbus.pusher     import Pusher
from otsim.msgbus.subscriber import Subscriber


class IO(HelicsFederate):
  def get_tag(key: str, tag: str) -> str:
    if tag: return tag

    tag = key.split('/')

    if len(tag) > 1:
      return tag[1]
    else:
      return tag[0]


  def get_type(typ: str) -> DataType:
    if typ == 'boolean':
      return DataType.boolean
    elif typ == 'double':
      return DataType.double
    elif typ == 'vector':
      return DataType.vector
    else:
      return None


  def default_vector_labels(n: int) -> typing.List[str]:
    # TODO: currently using letters to match power systems (A, B, C phases).
    # However, this is capped at 26 element vectors.
    if n > 26:
      raise ValueError(
        f"vector length {n} exceeds default alphabet of 26 labels; "
        f"configure <elements> explicitly"
      )
    return [chr(ord('a') + i) for i in range(n)]


  def __init__(self: IO, pub: str, pull: str, el: ET.Element):
    # mutex to protect access to updated values dictionary
    self.mutex = threading.Lock()
    # map updated tags --> values
    self.updated: typing.Dict[str, float] = {}

    # map HELICS topics --> ot-sim tags
    self.tags: typing.Dict[str, str] = {}
    # map ot-sim tags -- HELICS topics
    self.keys: typing.Dict[str, str] = {}
    # map HELICS topics --> HELICS pub
    self.pubs: typing.Dict[str, Publication] = {}
    # map HELICS topics --> HELICS sub
    self.subs: typing.Dict[str, Subscription] = {}
    # map ot-sim tags --> list of HELICS topic and endpoint
    self.eps: typing.Dict[str, typing.List[typing.Dict]] = defaultdict(list)
    # map HELICS topic --> ordered list of element labels for vector pub/sub
    self.vector_elements: typing.Dict[str, typing.List[str]] = {}
    # holding area for vector publication elements that arrive across cycles
    self.partial_vectors: typing.Dict[str, typing.Dict[str, float]] = defaultdict(dict)

    self.name = el.get('name', default='ot-sim-io')

    broker    = el.findtext('broker-endpoint',    default='127.0.0.1')
    log_level = el.findtext('federate-log-level', default='SUMMARY')

    HelicsFederate.federate_info_core_init_string = f'--federates=1 --broker={broker} --loglevel={log_level}'
    HelicsFederate.federate_info_log_level        = log_level

    name = el.findtext('federate-name', default=self.name)

    HelicsFederate.federate_name = name

    start = el.findtext('start-time', default=1)
    end   = el.findtext('end-time',   default=3600)

    HelicsFederate.start_time = int(start)
    HelicsFederate.end_time   = int(end)

    real_time = el.findtext('real-time', default='true')

    HelicsFederate.federate_info_real_time = real_time.lower() in ['true', 'yes', '1']

    for s in el.findall('subscription'):
      key = s.findtext('key')
      typ = IO.get_type(s.findtext('type'))
      tag = IO.get_tag(key, s.findtext('tag'))

      elements_text = s.findtext('elements')
      if typ == DataType.vector and elements_text:
        self.vector_elements[key] = [e.strip() for e in elements_text.split(',') if e.strip()]

      s = Subscription(key, typ)

      HelicsFederate.subscriptions.append(s)

      self.tags[key] = tag
      self.keys[tag] = key
      self.subs[key] = s

    for p in el.findall('publication'):
      key = p.findtext('key')
      typ = IO.get_type(p.findtext('type'))
      tag = IO.get_tag(key, p.findtext('tag'))

      if typ == DataType.vector:
        elements_text = p.findtext('elements')
        if not elements_text:
          raise ValueError(
            f"publication '{key}' has type 'vector' but is missing required <elements>"
          )
        self.vector_elements[key] = [e.strip() for e in elements_text.split(',') if e.strip()]

      p = Publication(key, typ)

      HelicsFederate.publications.append(p)

      self.tags[key] = tag
      self.keys[tag] = key
      self.pubs[key] = p

    need_endpoint = False

    for e in el.findall('endpoint'):
      need_endpoint = True

      endpoint_name = e.get('name')
      assert endpoint_name

      for t in e.findall('tag'):
        tag = t.text
        key = t.get('key', default=tag)

        self.eps[tag].append({'topic': key, 'endpoint': endpoint_name})

    if need_endpoint:
      # We only need a single endpoint in this HELICS federate to send updates
      # over. The name doesn't really matter since we'll never be receiving
      # updates, just sending them.
      HelicsFederate.endpoints.append(Endpoint("updates"))

    HelicsFederate.__init__(self, module_name=self.name)

    pub_endpoint  = el.findtext('pub-endpoint',  default=pub)
    pull_endpoint = el.findtext('pull-endpoint', default=pull)

    self.subscriber = Subscriber(pub_endpoint)
    self.pusher     = Pusher(pull_endpoint)
    self.metrics    = MetricsPusher()

    self.subscriber.add_update_handler(self.handle_msgbus_update)
    self.metrics.new_metric(MetricKind.COUNTER, 'status_count',            'number of OT-sim status messages generated')
    self.metrics.new_metric(MetricKind.COUNTER, 'update_count',            'number of OT-sim update messages processed')
    self.metrics.new_metric(MetricKind.COUNTER, 'helics_sub_count',        'number of HELICS subscriptions processed')
    self.metrics.new_metric(MetricKind.COUNTER, 'helics_pub_update_count', 'number of HELICS publication-based updates generated')


  def start(self: IO):
    # these already run in a thread
    self.subscriber.start('RUNTIME')
    self.metrics.start(self.pusher, self.name)

    # run HELICS helper in a thread
    threading.Thread(target=self.run, daemon=True).start()


  def stop(self: IO):
    self.subscriber.stop()
    self.metrics.stop()


  def handle_msgbus_update(self: IO, env: Envelope):
    update = envelope.update_from_envelope(env)

    if update:
      self.metrics.incr_metric('update_count')

      with self.mutex:
        for point in update['updates']:
          self.updated[point['tag']] = point['value']


  def action_subscriptions(self: IO, data: typing.Dict, _):
    '''published subcribed data as status message to message bus'''

    points: typing.List[Point] = []

    for k, v in data.items():
      tag = self.tags[k]

      if not tag: continue

      sub = self.subs[k]

      if sub.type == DataType.vector:
        if v is None: continue

        labels = self.vector_elements.get(k) or IO.default_vector_labels(len(v))

        if len(v) > len(labels):
          self.log(
            f'vector for {k} has {len(v)} elements but only {len(labels)} '
            f'labels configured; truncating to {len(labels)}'
          )

        for i, label in enumerate(labels):
          if i >= len(v): break
          points.append({'tag': f'{tag}.{label}', 'value': float(v[i]), 'ts': 0})
          self.metrics.incr_metric('helics_sub_count')
        continue

      points.append({'tag': tag, 'value': float(v), 'ts': 0})
      self.metrics.incr_metric('helics_sub_count')

    if len(points) > 0:
      env = envelope.new_status_envelope(self.name, {'measurements': points})
      self.pusher.push('RUNTIME', env)
      self.metrics.incr_metric('status_count')


  def action_publications(self: IO, data: typing.Dict, _):
    '''publish any update messages that have come in to HELICS'''

    with self.mutex:
      for k in data.keys():
        pub = self.pubs[k]
        tag = self.tags[k]

        if pub.type == DataType.vector:
          # vector publishes are assembled from per-element scalar tags. each
          # cycle we pull any newly-arrived elements out of self.updated into
          # self.partial_vectors, then publish atomically once all elements
          # for the vector are present.
          labels = self.vector_elements[k]

          for label in labels:
            element_tag = f'{tag}.{label}'
            if element_tag in self.updated:
              self.partial_vectors[k][label] = self.updated[element_tag]

          if all(label in self.partial_vectors[k] for label in labels):
            data[k] = [float(self.partial_vectors[k][label]) for label in labels]
            self.partial_vectors[k].clear()
            self.log(f'updating federate topic {k} to {data[k]}')
            self.metrics.incr_metric('helics_pub_update_count')
          elif self.partial_vectors[k]:
            present = sorted(self.partial_vectors[k].keys())
            missing = [l for l in labels if l not in self.partial_vectors[k]]
            self.log(
              f'holding partial vector for {k}: have {present}, waiting for {missing}'
            )

          continue

        if tag and tag in self.updated:
          value = self.updated[tag]

          self.log(f'updating federate topic {k} to {value}')

          if pub.type == DataType.boolean:
            data[k] = bool(value)
          elif pub.type == DataType.double:
            data[k] = float(value)
          else:
            continue

          self.metrics.incr_metric('helics_pub_update_count')

      self.updated.clear()


  def action_endpoints_send(self: IO, endpoints: typing.Dict, ts: int):
    '''send any update messages that have come in to HELICS'''

    self.log('checking for queued messages to send to remote endpoints')

    with self.mutex:
      # key is endpoint name, value is list of dictionaries representing updates
      data = defaultdict(list)

      for tag in self.updated:
        self.log(f'tag {tag} has been updated')

        if tag in self.eps:
          self.log(f'remote endpoint(s) found for tag {tag}')

          for ep in self.eps[tag]:
            endpoint = ep['endpoint']
            key      = ep['topic']
            value    = self.updated[tag]

            self.log(f'updating federate topic {key} to {value} in remote endpoint {endpoint}')

            data[endpoint].append({'tag': key, 'value': value})
        else:
          self.log(f'tag {tag} not configured for any remote endpoints')

      for endpoint, data in data.items():
        data = json.dumps(data)

        self.log(f'sending {data} to endpoint {endpoint}')

        endpoints['updates'].append(Message(destination=endpoint, time=ts, data=data))

      self.updated.clear()


  def action_endpoints_recv(self: IO, endpoints: typing.Dict, ts: int):
    pass


def main():
  if len(sys.argv) < 2:
    print('no config file provided')
    sys.exit(1)

  tree = ET.parse(sys.argv[1])

  root = tree.getroot()
  assert root.tag == 'ot-sim'

  mb = root.find('message-bus')

  if mb:
    pub  = mb.findtext('pub-endpoint')
    pull = mb.findtext('pull-endpoint')
  else:
    pub  = 'tcp://127.0.0.1:5678'
    pull = 'tcp://127.0.0.1:1234'

  devices: typing.List[IO] = []

  for io in root.findall('io'):
    device = IO(pub, pull, io)
    device.start()

    devices.append(device)
  
  waiter = threading.Event()

  def handler(*_):
    waiter.set()

  signal.signal(signal.SIGINT, handler)
  waiter.wait()

  for device in devices:
    device.stop()
