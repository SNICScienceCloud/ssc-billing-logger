#!/usr/bin/env python3

import arrow
import getopt
import itertools
import json
from xml.etree import cElementTree as ET
import pprint
import requests
import os
import sys

# import sqlite3

from functools import partial
from urllib.parse import urlencode

DEFAULT_CONFIG_FILENAME = "/etc/ssc-billing-extract.conf"
ppr = pprint.PrettyPrinter(indent=4, stream=sys.stderr)


class Config:
    def __init__(self, filename = DEFAULT_CONFIG_FILENAME):
        with open(filename, 'r') as f:
            cfg = json.load(f)
            self.username = cfg['username']
            self.password = cfg['password']
            self.project = cfg['project']
            self.domain = cfg['domain']
            self.keystone_url = cfg['keystone_url']
            self.ceilometer_url = cfg['ceilometer_url']
            self.socks_proxy_url = cfg.get('socks_proxy_url', None)
            self.site = cfg['site']
            self.region = cfg['region']
            self.datadir = cfg['datadir']


    def valid(self):
        return self.username is not None and self.password is not None and self.project is not None and self.domain is not None and \
               self.keystone_url is not None and self.ceilometer_url is not None and self.datadir is not None

class CostDefinition:
    def __init__(self, region, dirname):
        self.flavor_costs = {}
        cost_filename = os.path.join(dirname, "state/costs.json")
        try:
            with open(cost_filename, 'r') as f:
                res = json.load(f)
                reg = res['regions'][region]
                self.flavor_costs = reg

        except FileNotFoundError:
            raise RuntimeError('[E1005] Could not read cost source %s' % cost_filename)
        except KeyError:
            raise RuntimeError('[E1006] Could not parse cost definition file')

    def lookup(self, flavor_name):
        try:
            return self.flavor_costs[flavor_name]
        except KeyError:
            sys.stderr.write('Cost requested for flavour %s which is not defined\n' % (flavour_name,))
            return 0.0

class PersistentState:
    def __init__(self, dirname):
        self.state_filename = os.path.join(dirname, "state/state.json")
        self.last_timepoint = None
        try:
            with open(self.state_filename, 'r') as file:
                res = json.load(file)
                timepoint = res.get('last_timepoint', None)
                if timepoint is not None:
                    self.last_timepoint = arrow.get(timepoint)
        except FileNotFoundError:
            pass

    def write(self):
        out = {}
        if self.last_timepoint is not None:
            out['last_timepoint'] = self.last_timepoint.to('utc').isoformat()
        with open(self.state_filename, 'w') as f:
            json.dump(out, f)


CR_NAMESPACE = "http://sams.snic.se/namespaces/2016/04/cloudrecords"
CLOUD_COMPUTE_RECORD = ET.QName("{%s}CloudComputeRecord" % CR_NAMESPACE)
ET.register_namespace('cr', CR_NAMESPACE)

class ComputeRecord:
    def qname(name):
        return ET.QName('{%s}%s' % (CR_NAMESPACE, name))

    def __init__(self, cfg):
        self.RecordIdentity = None
        self.Site = cfg.site
        self.Project = None
        self.User = None
        self.InstanceId = None
        self.StartTime = None
        self.EndTime = None
        self.Duration = None
        self.Region = cfg.region
        self.Zone = None
        self.Flavour = None
        self.Cost = None
        self.AllocatedCPU = None
        self.AllocatedDisk = None
        self.AllocatedMemory = None

        self.UsedCPU = None
        self.UsedMemory = None
        self.UsedNetworkUp = None
        self.UsedNetworkDown = None
        self.IOPS = None

    def json(self):
        return json.dumps(dict(RecordIdentity=self.RecordIdentity,
                                Site=self.Site,
                                Project=self.Project,
                                User=self.User,
                                InstanceId=self.InstanceId,
                                StartTime=self.StartTime.isoformat(),
                                EndTime=self.EndTime.isoformat(),
                                Duration=self.Duration,
                                Region=self.Region,
                                Zone=self.Zone,
                                Flavour=self.Flavour,
                                Cost=self.Cost,
                                AllocatedCPU=self.AllocatedCPU,
                                AllocatedDisk=self.AllocatedDisk,
                                AllocatedMemory=self.AllocatedMemory,

                                UsedCPU=self.UsedCPU,
                                UsedMemory=self.UsedMemory,
                                UsedNetworkUp=self.UsedNetworkUp,
                                UsedNetworkDown=self.UsedNetworkDown,
                                IOPS=self.IOPS))

    def recordid(self):
        return "ssc/%s/cr/%s/%s" % (self.Site, self.InstanceId, self.EndTime.timestamp)

    def xml(self):
        def add_sub_element(element, tag_name, value):
            ret = ET.SubElement(element, ComputeRecord.qname(tag_name))
            ret.text = str(value)
            return ret

        def add_sub_element_int(element, tag_name, value):
            ret = ET.SubElement(element, ComputeRecord.qname(tag_name))
            ret.text = str(int(value))
            return ret

        def add_sub_element_with_default(element, tag_name, value, default=0):
            ret = ET.SubElement(element, ComputeRecord.qname(tag_name))
            if value is not None:
                ret.text = str(value)
            else:
                ret.text = str(default)
            return ret

        def add_sub_element_with_default_int(element, tag_name, value, default=0):
            ret = ET.SubElement(element, ComputeRecord.qname(tag_name))
            if value is not None:
                ret.text = str(int(value))
            else:
                ret.text = str(int(default))
            return ret

        def add_sub_element_if_not_none(element, tag_name, value):
            if value is not None:
                return add_sub_element(element, tag_name, value)

        def add_sub_element_if_not_none_int(element, tag_name, value):
            if value is not None:
                return add_sub_element(element, tag_name, value)

        def add_attribute(element, name, value):
            element.set(ComputeRecord.qname(name), value)

        root = ET.Element(ComputeRecord.qname('CloudComputeRecord'))
        ri = add_sub_element(root, 'RecordIdentity', '')
        add_attribute(ri, 'createTime', arrow.now().to('utc').isoformat())
        add_attribute(ri, 'recordId', self.recordid())
        add_sub_element(root, 'Site', self.Site)
        add_sub_element(root, 'Project', self.Project)
        add_sub_element(root, 'User', self.User)
        add_sub_element(root, 'InstanceId', self.InstanceId)
        add_sub_element(root, 'StartTime', self.StartTime.to('utc').isoformat())
        add_sub_element(root, 'EndTime', self.EndTime.to('utc').isoformat())
        add_sub_element(root, 'Duration', self.Duration)
        add_sub_element(root, 'Region', self.Region)
        add_sub_element(root, 'Zone', self.Zone)
        add_sub_element(root, 'Flavour', self.Flavour)
        add_sub_element_with_default(root, 'Cost', self.Cost)
        add_sub_element_with_default(root, 'AllocatedCPU', self.AllocatedCPU)
        add_sub_element_with_default_int(root, 'AllocatedDisk', self.AllocatedDisk)
        add_sub_element_with_default_int(root, 'AllocatedMemory', self.AllocatedMemory)
        add_sub_element_if_not_none(root, 'UsedCPU', self.UsedCPU)
        add_sub_element_if_not_none_int(root, 'UsedMemory', self.UsedCPU)
        add_sub_element_if_not_none_int(root, 'UsedNetworkUp', self.UsedNetworkUp)
        add_sub_element_if_not_none_int(root, 'UsedNetworkDown', self.UsedNetworkDown)
        add_sub_element_if_not_none_int(root, 'IOPS', self.IOPS)
        return root

def http_category(code):
    return code // 100 * 100

class OpenStack:
    def __init__(self, cfg):
        self.proxies = dict(http=cfg.socks_proxy_url) if cfg.socks_proxy_url else None
        self.get = partial(requests.get, proxies=self.proxies)
        self.post = partial(requests.post, proxies=self.proxies)

        self.keystone_url = cfg.keystone_url
        self.ceilometer_url = cfg.ceilometer_url

        ar = self.get(self.keystone_url)
        if http_category(ar.status_code) != 200:
            raise RuntimeError("[E1000] No OK response from keystone")

        auth_scoped_payload = json.dumps({'auth': {
            'identity': {
                'methods': ['password'],
                'password': {
                    'user': {
                        'name': cfg.username,
                        'password': cfg.password,
                        'domain': {'id': cfg.domain},
                    }
                }
            },
            'scope': {
                'project': {
                    'domain': {'id': cfg.domain},
                    'name': cfg.project
                }
            }
        }})

        ar = self.post(self.keystone_url + '/auth/tokens', data=auth_scoped_payload)
        if http_category(ar.status_code) != 200:
            raise RuntimeError("[E1001] Could not fetch authorization token from keystone")
        token_info = json.loads(ar.text)
        self.service_catalog = token_info['token']['catalog']

        def find_compute_url(cfg, catalog):
            for svc in catalog:
                if svc['name'] == 'nova' and svc['type'] == 'compute':
                    for ep in svc['endpoints']:
                        if ep['region'] == cfg.region and ep['interface'] == 'admin':
                            return ep['url']

        self.compute_url = find_compute_url(cfg, self.service_catalog)

        admin_scoped_token = ar.headers['X-Subject-Token']
        self.scoped_get = partial(self.get, headers={'X-Auth-Token': admin_scoped_token})
        self.scoped_post = partial(self.post, headers={'X-Auth-Token': admin_scoped_token})


class MeterSet:
    def __init__(self, openstack):
        ar = openstack.scoped_get(openstack.ceilometer_url + '/meters')
        if http_category(ar.status_code) != 200:
            raise RuntimeError("[E1002] Could not fetch meters from ceilometer")
        meters = json.loads(ar.text)
        if len(meters) == 0:
            sys.stderr.write("[W1000] Ceilometer meters collection is empty, services may need restarting\n");

        self.resource_infos = {}
        for meter in meters:
            res_id = meter['resource_id']
            proj_id = meter['project_id']
            user_id = meter['user_id']
            self.resource_infos[res_id] = {'user_id': user_id, 'project_id': proj_id}

        self.valid_meters_by_project = {}
        meters = list(sorted(meters, key=lambda x: x['project_id']))
        for (proj_id, group) in itertools.groupby(meters, lambda x: x['project_id']):
            ar = openstack.scoped_get(openstack.keystone_url + '/projects/%s' % proj_id)
            if http_category(ar.status_code) == 200:
                proj_info = json.loads(ar.text)
                self.valid_meters_by_project[proj_id] = {
                    'proj_info': proj_info,
                    'meters': list(filter(lambda x: x['name'] == 'vcpus' or x['name'] == 'memory', group))
                }
            elif http_category(ar.status_code) == 400:
                ppr.pprint("project %s is missing" % (proj_id,))


def populate_instances(openstack, period_start, period_end):            
    def mkquery(field, op, value):
        return [('q.field', field), ('q.op', op), ('q.value', value)]
        
    instance_measurements = {}
    ar = openstack.scoped_get(openstack.ceilometer_url + '/resources')
    if http_category(ar.status_code) != 200:
        raise RuntimeError("[E1004] Could not fetch resources from ceilometer")
    resources = json.loads(ar.text)
    if len(resources) == 0:
            sys.stderr.write("[W1001] Ceilometer resources collection is empty, services may need restarting\n");

    for (meter, field) in [("vcpus", 'AllocatedCPU'), ("memory", 'AllocatedMemory')]:
        stat_url = '%s/meters/%s/statistics' % (openstack.ceilometer_url, meter)

        duration_seconds = 3600
        duration_iso = 'PT%dS' % (duration_seconds,)

        stat_params = [('groupby', 'resource_id'),
                        ('groupby', 'project_id'),
                        ('groupby', 'user_id'),
                        ('period', duration_seconds)] + \
                        mkquery('timestamp', 'gt', period_start) + \
                        mkquery('timestamp', 'le', period_end)

        ar = openstack.scoped_get(stat_url, params=stat_params)
        if http_category(ar.status_code) == 200:
            res = json.loads(ar.text)
            for entry in res:
                res_id = entry['groupby']['resource_id']

                start_time = arrow.get(entry['period_start'])
                end_time = arrow.get(entry['period_end'])
                key = (res_id, start_time, end_time)
                inst = None
                if key in instance_measurements:
                    inst = instance_measurements[key]
                else:
                    try:
                        res_res = next(r for r in resources if r['resource_id'] == res_id)
                        metadata = res_res['metadata']

                        inst = instance_measurements[key] = {}
                        inst['InstanceId'] = metadata['instance_id']
                        inst['Flavor'] = metadata['instance_type']
                        inst['ResourceId'] = res_id
                        inst['ProjectId'] = res_res['project_id']
                        inst['UserId'] = res_res['user_id']
                        inst['StartTime'] = arrow.get(entry['period_start'])
                        inst['EndTime'] = arrow.get(entry['period_end'])
                        inst['Duration'] = duration_iso
                        inst['Zone'] = metadata.get('availability_zone', 'default')
                    except StopIteration:
                        pass

                if inst:
                    inst[field] = entry['max']
    return instance_measurements

def gather_cloud_records(openstack, cfg, instance_measurements, cost_definition):
    # Required: RecordIdentity, Site, Project, User, InstanceId, StartTime, EndTime, Duration, Region, Zone,
    #           Flavour, Cost, AllocatedCPU, AllocatedDisk, AllocatedMemory
    # Optional: UsedCPU, UsedMemory, UsedNetworkUp, UsedNetworkDown, IOPS

    users = {}
    projects = {}
    flavors = {}

    ar = openstack.scoped_get(openstack.compute_url + '/flavors')
    if http_category(ar.status_code) != 200:
        raise RuntimeError("[E1003] Could not fetch flavors from compute")

    for flavor in json.loads(ar.text)['flavors']:
        flavors[flavor['id']] = flavor

    crs = []
    for rid, inst in instance_measurements.items():
        uid = inst['UserId']
        pid = inst['ProjectId']
        if pid not in projects:
            ar = openstack.scoped_get(openstack.keystone_url + '/projects/%s' % pid)
            if http_category(ar.status_code) != 200:
                next
            projects[pid] = json.loads(ar.text)
        if uid not in users:
            ar = openstack.scoped_get(openstack.keystone_url + '/users/%s' % uid)
            if http_category(ar.status_code) != 200:
                next
            users[uid] = json.loads(ar.text)
        proj = projects[pid]
        try:
            flavor_name = inst['Flavor']
            cr = ComputeRecord(cfg)
            cr.InstanceId = inst['ResourceId']
            cr.Project = proj['project']['name']
            cr.User = users[uid]['user']['name']
            cr.AllocatedCPU = inst['AllocatedCPU']
            cr.AllocatedMemory = inst['AllocatedMemory']
            cr.StartTime = inst['StartTime']
            cr.EndTime = inst['EndTime']
            cr.Duration = inst['Duration']
            cr.Zone = inst['Zone']
            cr.Flavour = flavor_name
            cr.Cost = cost_definition.lookup(flavor_name)
            crs.append(cr)
        except KeyError as e:
            ppr.pprint(e)
            ppr.pprint(inst)
            raise

    return crs

def write_cloud_records(cfg, datetime_of_run, records):
    root = ET.Element(ComputeRecord.qname('CloudRecords'))
    for cr in records:
        x = cr.xml()
        root.append(x)

    xml_leaf_name = datetime_of_run.to('UTC').strftime('%Y%m%dT%H%MZ.xml')
    xml_filename = os.path.join(cfg.datadir, 'records', xml_leaf_name)
    with open(xml_filename, 'w') as f:
        tree = ET.ElementTree(root)
        tree.write(f, encoding='unicode', xml_declaration=True)

    """
    with sqlite3.connect(os.path.join(cfg.datadir, 'dump.db')) as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS recs
                    (record_create_time TEXT, record_id TEXT, site TEXT, project TEXT, user TEXT,
                    instance_id TEXT, start_time TEXT, end_time TEXT,
                    duration INTEGER, region TEXT, zone TEXT, flavour TEXT,
                    cost REAL, allocated_cpu REAL, allocated_disk REAL,
                    allocated_memory REAL, used_cpu REAL, used_memory REAL,
                    used_network_up REAL, used_network_down REAL, iops REAL)''')

        create_time = arrow.utcnow().isoformat()

        for cr in records:
            tup = (create_time, cr.recordid(), cr.Site, cr.Project, cr.User,
                   cr.InstanceId, cr.StartTime.to('utc').isoformat(), cr.EndTime.to('utc').isoformat(),
                   cr.Duration, cr.Region, cr.Zone, cr.Flavour,
                   cr.Cost, cr.AllocatedCPU, cr.AllocatedDisk,
                   cr.AllocatedMemory, cr.UsedCPU, cr.UsedMemory,
                   cr.UsedNetworkUp, cr.UsedNetworkDown, cr.IOPS)
            ppr.pprint(tup)
            c.execute('''INSERT INTO recs VALUES (
                        ?, ?, ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?)''', tup)

        conn.commit()
        """

def main():
    opts, args = getopt.getopt(sys.argv[1:], "c:")

    cfg_filename = None
    for (k, v) in opts:
        if k == '-c':
            cfg_filename = v
    cfg = Config(cfg_filename)

    cost_definition = CostDefinition(cfg.region, cfg.datadir)
    persistent_state = PersistentState(cfg.datadir)

    last_full_report_timepoint = persistent_state.last_timepoint
    if last_full_report_timepoint is None:
        last_full_report_timepoint = arrow.get('2015-01-01T00:00Z')

    period_start = last_full_report_timepoint
    period_end = arrow.utcnow().floor('hour')
    if period_end <= period_start:
        return

    openstack = OpenStack(cfg)
    meters = MeterSet(openstack)
    # ppr.pprint(('valid meters by project', meters.valid_meters_by_project))

    # Required: RecordIdentity, Site, Project, User, InstanceId, StartTime, EndTime,
    #           Duration, Region, Zone, Flavour, Cost, AllocatedCPU, AllocatedDisk,
    #           AllocatedMemory
    # Optional: UsedCPU, UsedMemory, UsedNetworkUp, UsedNetworkDown, IOPS

    instance_measurements = populate_instances(openstack, period_start, period_end)
    # ppr.pprint(('instance measurements', instance_measurements))
    cloud_records = gather_cloud_records(openstack, cfg, instance_measurements, cost_definition)

    write_cloud_records(cfg, period_end, cloud_records)

#   persistent_state.last_timepoint = period_end
#   persistent_state.write()

main()
