#!/usr/bin/python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Deploys a new Impala_Kudu service, either based on an existing Impala service
# or from scratch.
#
# Prerequisites:
# - A cluster running CDH 5.4.x and Cloudera Manager 5.4.x with x >= 7
# - CM API Python client (http://cloudera.github.io/cm_api/docs/python-client).
#
# Sample usage:
#
#   ./deploy.py clone IMPALA_KUDU IMPALA-1
#   Clones IMPALA-1 into a new Impala_Kudu service called "IMPALA_KUDU".
#
#   ./deploy.py create new_service /data/impala/
#   Creates a new Impala_Kudu service called "new_service" using /data/impala/
#   for its scratch directories.

import argparse
import hashlib
import os
import re
import time

from cm_api.api_client import ApiResource

IMPALA_KUDU_PARCEL_URL = os.getenv("IMPALA_KUDU_PARCEL_URL",
                                   "http://archive.cloudera.com/beta/impala-kudu/parcels/latest")
IMPALA_KUDU_PARCEL_PRODUCT = "IMPALA_KUDU"
MAX_PARCEL_REPO_WAIT_SECS = 60
MAX_PARCEL_WAIT_SECS = 60 * 30

SERVICE_DEPENDENCIES = {
    "HDFS" : True,
    "HIVE" : True,
    "YARN" : False,
    "HBASE" : False,
    "SENTRY" : False,
    "ZOOKEEPER" : False
}

def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--host", type=str,
                        default="localhost",
                        help="Hostname of the Cloudera Manager server.")
    parser.add_argument("--user", type=str,
                        default="admin",
                        help="Username with which to log into Cloudera Manager.")
    parser.add_argument("--password", type=str,
                        default="admin",
                        help="Password with which to log into Cloudera Manager.")
    parser.add_argument("--cluster", type=str,
                        help="Name of existing cluster where the Impala_Kudu service "
                        "should be added. If not specified, uses the only cluster or "
                        "raises an exception if multiple clusters are found.")
    parents_parser = argparse.ArgumentParser(add_help=False)
    parents_parser.add_argument("service_name", type=str,
                                help="Name of Impala_Kudu service to create.")
    subparsers = parser.add_subparsers(dest="subparsers_name")
    clone_parser = subparsers.add_parser("clone",
                                         parents=[parents_parser],
                                         help="Use an existing Impala service as a template for "
                                         "the new Impala_Kudu service. To be used when Impala_"
                                         "Kudu is to run side-by-side with an existing Impala.")
    clone_parser.add_argument("based_on", type=str,
                              help="Name of existing Impala service to clone as the basis for the "
                              "new service.")
    create_parser = subparsers.add_parser("create",
                                          parents=[parents_parser],
                                          help="create a new Impala_Kudu service from scratch. To "
                                          "be used when Impala_Kudu runs in its own cluster.")
    create_parser.add_argument("--master_host", type=str,
                               help="Hostname where new Impala_Kudu service's master roles should "
                               "be placed. If not specified, uses the Cloudera Manager Server host "
                               "or raises an exception if that host is not managed.")
    for service_type, required in SERVICE_DEPENDENCIES.iteritems():
        create_parser.add_argument("--%s_dependency" % (service_type.lower(),),
                                   type=str,
                                   help="Name of %s service that the new Impala_Kudu service "
                                   "should depend on. If not specified, will use only service of "
                                   "that type in the cluster. Will raise an exception if exactly "
                                   "one instance of that service is not found in the cluster. %s" %
                                   (service_type, "REQUIRED." if required else ""))
    create_parser.add_argument("scratch_dirs", type=str,
                               help="Comma-separated list of scratch directories to use in the new "
                               "Impala_Kudu service.")

    return parser.parse_args()

def find_cluster(api, cluster_name):
    if cluster_name:
        cluster = api.get_cluster(cluster_name)
    else:
        all_clusters = api.get_all_clusters()
        if len(all_clusters) == 0:
            raise Exception("No clusters found; create one before calling this script")
        if len(all_clusters) > 1:
            raise Exception("Cannot use implicit cluster; there is more than one available")
        cluster = all_clusters[0]
    print("Found cluster: %s" % (cluster.displayName, ))
    return cluster

def find_dependencies(args, cluster):
    deps = []

    # { service type : { service name : service }}
    services_by_type = {}
    for service in cluster.get_all_services():
        service_dict = services_by_type.get(service.type, {})
        service_dict[service.name] = service
        services_by_type[service.type] = service_dict

    for service_type, required in SERVICE_DEPENDENCIES.iteritems():
        candidates = services_by_type.get(service_type, {})
        arg = getattr(args, service_type.lower() + "_dependency")
        if arg:
            found = candidates.get(arg, None)
            if not found:
                raise Exception("Could not find dependency service (type %s, name %s)" %
                                (service_type, arg))
            print "Found explicit dependency service %s" % (found.name)
            deps.append(found)
        else:
            if not required:
                print "Skipping optional dependency of type %s" % (service_type,)
                continue
            if len(candidates) > 1:
                raise Exception("Found %d possible implicit dependency services of type %s" %
                                (len(candidates), service_type))
            elif len(candidates) == 0:
                raise Exception("Could not find implicit dependency service of type %s" %
                                (service_type,))
            else:
                found = candidates.values()[0]
                print "Found implicit dependency service %s" % (found.name,)
                deps.append(found)
    return deps

def check_new_service_does_not_exist(api, cluster, new_name):
    for service in cluster.get_all_services():
        if service.displayName == new_name:
            raise Exception("New service name %s already in use" % (new_name,))

    print "New service name %s is not in use" % (new_name,)

def find_template_service(api, cluster, based_on):
    template_service = None
    for service in cluster.get_all_services():
        if based_on and service.displayName == based_on:
            if service.type != "IMPALA":
                raise Exception("Based-on service %s is of wrong type %s" %
                                (based_on, service.type))
            print "Found based-on service: %s" % (based_on,)
            template_service = service

    if based_on and not template_service:
        raise Exception("Could not find based-on service: %s" % (based_on,))

    return template_service

def find_master_host(api, cm_hostname, master_hostname):
    for h in api.get_all_hosts():
        if master_hostname and h.hostname == master_hostname:
            print "Found master host %s" % (master_hostname,)
            return h
        elif not master_hostname and h.hostname == cm_hostname:
            print "Found implicit master host on CM host %s" % (cm_hostname,)
            return h

    if master_hostname:
        raise Exception("Could not find master host with hostname %s" % (master_hostname,))
    else:
        raise Exception("Could not find implicit master host %s" % (cm_hostname,))

def get_best_parcel(api, cluster):
    parcels_available_remotely = []
    parcels_downloaded = []
    parcels_distributed = []
    parcels_activated = []
    for parcel in cluster.get_all_parcels():
        if parcel.product == IMPALA_KUDU_PARCEL_PRODUCT:
            if parcel.stage == "AVAILABLE_REMOTELY":
                parcels_available_remotely.append(parcel)
            elif parcel.stage == "DOWNLOADED":
                parcels_downloaded.append(parcel)
            elif parcel.stage == "DISTRIBUTED":
                parcels_distributed.append(parcel)
            elif parcel.stage == "ACTIVATED":
                parcels_activated.append(parcel)

    def parcel_cmp(p1, p2):
        if p1.version < p2.version:
            return -1
        elif p1.version > p2.version:
            return 1
        else:
            return 0

    # Prefer the "closest" parcel, even if it's not the newest by version.
    if len(parcels_activated) > 0:
        parcel = sorted(parcels_activated, key=lambda parcel: parcel.version)[0]
    elif len(parcels_distributed) > 0:
        parcel = sorted(parcels_distributed, key=lambda parcel: parcel.version)[0]
    elif len(parcels_downloaded) > 0:
        parcel = sorted(parcels_downloaded, key=lambda parcel: parcel.version)[0]
    elif len(parcels_available_remotely) > 0:
        parcel = sorted(parcels_available_remotely, key=lambda parcel: parcel.version)[0]
    else:
        parcel = None

    if parcel:
        print "Chose best parcel %s-%s (stage %s)" % (parcel.product,
                                                      parcel.version,
                                                      parcel.stage)
    else:
        print "Found no candidate parcels"

    return parcel

def ensure_parcel_repo_added(api):
    cm = api.get_cloudera_manager()
    config = cm.get_config(view='summary')
    parcel_urls = config.get("REMOTE_PARCEL_REPO_URLS", "").split(",")
    if IMPALA_KUDU_PARCEL_URL in parcel_urls:
        print "Impala_Kudu parcel URL already present"
    else:
        print "Adding Impala_Kudu parcel URL"
        parcel_urls.append(IMPALA_KUDU_PARCEL_URL)
        config["REMOTE_PARCEL_REPO_URLS"] = ",".join(parcel_urls)
        cm.update_config(config)

def wait_for_parcel_stage(cluster, parcel, stage):
    for attempt in xrange(1, MAX_PARCEL_WAIT_SECS + 1):
        new_parcel = cluster.get_parcel(parcel.product, parcel.version)
        if new_parcel.stage == stage:
            return
        if new_parcel.state.errors:
            raise Exception(str(new_parcel.state.errors))
        print "progress: %s / %s" % (new_parcel.state.progress,
                                     new_parcel.state.totalProgress)
        time.sleep(1)
    else:
        raise Exception("Parcel %s-%s did not reach stage %s in %d seconds" %
                        (parcel.product, parcel.version, stage, MAX_PARCEL_WAIT_SECS,))

def ensure_parcel_activated(cluster, parcel):
    parcel_stage = parcel.stage
    if parcel_stage == "AVAILABLE_REMOTELY":
        print "Downloading parcel: %s-%s " % (parcel.product, parcel.version)
        parcel.start_download()
        wait_for_parcel_stage(cluster, parcel, "DOWNLOADED")
        print "Downloaded parcel: %s-%s " % (parcel.product, parcel.version)
        parcel_stage = "DOWNLOADED"
    if parcel_stage == "DOWNLOADED":
        print "Distributing parcel: %s-%s " % (parcel.product, parcel.version)
        parcel.start_distribution()
        wait_for_parcel_stage(cluster, parcel, "DISTRIBUTED")
        print "Distributed parcel: %s-%s " % (parcel.product, parcel.version)
        parcel_stage = "DISTRIBUTED"
    if parcel_stage == "DISTRIBUTED":
        print "Activating parcel: %s-%s " % (parcel.product, parcel.version)
        parcel.activate()
        wait_for_parcel_stage(cluster, parcel, "ACTIVATED")
        print "Activated parcel: %s-%s " % (parcel.product, parcel.version)
        parcel_stage = "ACTIVATED"

    print "Parcel %s-%s is activated" % (parcel.product, parcel.version)

def print_configs(entity_name, config_dict):
    for attr, value in config_dict.iteritems():
        print "Set %s config %s=\'%s\'" % (entity_name, attr, value)

def create_new_service(api, cluster, new_name, deps, scratch_dirs, master_host):
    new_service = cluster.create_service(new_name, "IMPALA")
    print "Created new service %s" % (new_name,)

    service_config = {}
    for d in deps:
        service_config[d.type.lower() + "_service"] = d.name
    service_config["impala_service_env_safety_valve"] = "IMPALA_KUDU=1"
    new_service.update_config(service_config)
    print_configs("service " + new_name, service_config)

    for rcg in new_service.get_all_role_config_groups():
        if rcg.roleType == "IMPALAD":
            scratch_dirs_dict = { "scratch_dirs" : scratch_dirs }
            rcg.update_config(scratch_dirs_dict)
            print_configs("rcg " + rcg.displayName, scratch_dirs_dict)
            for h in cluster.list_hosts():
                if h.hostId == master_host.hostId:
                    continue

                # This formula is embedded within CM. If we don't strictly
                # adhere to it, we can't use any %s-%s-%s naming scheme.
                md5 = hashlib.md5()
                md5.update(h.hostId)
                new_role_name = "%s-%s-%s" % (new_name, rcg.roleType, md5.hexdigest())
                new_service.create_role(new_role_name, rcg.roleType, h.hostId)
                print "Created new role %s" % (new_role_name,)
        else:
            md5 = hashlib.md5()
            md5.update(master_host.hostId)
            new_role_name = "%s-%s-%s" % (new_name, rcg.roleType, md5.hexdigest())
            new_service.create_role(new_role_name, rcg.roleType, master_host.hostId)
            print "Created new role %s" % (new_role_name,)

def transform_path(rcg_name, rcg_config_dict, rcg_config_name):
    # TODO: Do a better job with paths where the role type is embedded.
    #
    # e.g. /var/log/impalad/lineage --> /var/log/impalad2/lineage
    val = rcg_config_dict.get(rcg_config_name, None)
    if not val:
        raise Exception("Could not get %s config for rcg %s" %
                        (rcg_config_name, rcg_name,))
    new_val = re.sub(r"/(.*?)(/?)$", r"/\g<1>2\g<2>", val)
    return {rcg_config_name : new_val}

def transform_port(rcg_name, rcg_config_dict, rcg_config_name):
    # TODO: Actually resolve all port conflicts.
    val = rcg_config_dict.get(rcg_config_name, None)
    if not val:
        raise Exception("Could not get %s config for rcg %s" %
                        (rcg_config_name, rcg_name,))
    try:
        val_int = int(val)
    except ValueError, e:
        raise Exception("Could not convert %s config (%s) for rcg %s into integer" %
                        (rcg_config_name, val, rcg_name))

    new_val = str(val_int + 7)
    return {rcg_config_name : new_val}

def transform_rcg_config(rcg):
    summary = rcg.get_config()
    full = {}
    for name, config in rcg.get_config("full").iteritems():
        full[name] = config.value if config.value else config.default

    new_config = summary

    if rcg.roleType == "IMPALAD":
        new_config.update(transform_path(rcg.name, full, "audit_event_log_dir"))
        new_config.update(transform_path(rcg.name, full, "lineage_event_log_dir"))
        new_config.update(transform_path(rcg.name, full, "log_dir"))
        new_config.update(transform_path(rcg.name, full, "scratch_dirs"))

        new_config.update(transform_port(rcg.name, full, "be_port"))
        new_config.update(transform_port(rcg.name, full, "beeswax_port"))
        new_config.update(transform_port(rcg.name, full, "hs2_port"))
        new_config.update(transform_port(rcg.name, full, "impalad_webserver_port"))
        new_config.update(transform_port(rcg.name, full, "state_store_subscriber_port"))
    elif rcg.roleType == "CATALOGSERVER":
        new_config.update(transform_path(rcg.name, full, "log_dir"))

        new_config.update(transform_port(rcg.name, full, "catalog_service_port"))
        new_config.update(transform_port(rcg.name, full, "catalogserver_webserver_port"))
    elif rcg.roleType == "STATESTORE":
        new_config.update(transform_path(rcg.name, full, "log_dir"))

        new_config.update(transform_port(rcg.name, full, "state_store_port"))
        new_config.update(transform_port(rcg.name, full, "statestore_webserver_port"))

    return new_config

def clone_existing_service(cluster, new_name, template_service):
    new_service = cluster.create_service(new_name, "IMPALA")
    print "Created new service %s" % (new_name,)

    service_config, _ = template_service.get_config()
    service_config["impala_service_env_safety_valve"] = "IMPALA_KUDU=1"
    new_service.update_config(service_config)
    print_configs("service " + new_name, service_config)

    saved_special_port = None
    i = 0
    for old_rcg in template_service.get_all_role_config_groups():
        if old_rcg.name != ("%s-%s-BASE" % (template_service.name, old_rcg.roleType)):
            new_rcg_name = "%s-%s-%d" % (new_name, old_rcg.roleType, i)
            i += 1
            new_rcg = new_service.create_role_config_group(new_rcg_name,
                                                           new_rcg_name,
                                                           old_rcg.roleType)
            print "Created new rcg %s" % (new_rcg_name,)
        else:
            new_rcg = new_service.get_role_config_group("%s-%s-BASE" % (new_name,
                                                                        old_rcg.roleType))
        new_rcg_config = transform_rcg_config(old_rcg)
        new_rcg.update_config(new_rcg_config)
        print_configs("rcg " + new_rcg.displayName, new_rcg_config)
        special_port = new_rcg_config.get("state_store_subscriber_port", None)
        if special_port:
            saved_special_port = special_port

        new_role_names = []
        for old_role in old_rcg.get_all_roles():
            md5 = hashlib.md5()
            md5.update(old_role.hostRef.hostId)
            new_role_name = "%s-%s-%s" % (new_name, new_rcg.roleType, md5.hexdigest())
            new_role = new_service.create_role(new_role_name,
                                               new_rcg.roleType,
                                               old_role.hostRef.hostId)
            print "Created new role %s" % (new_role_name,)
            new_role_names.append(new_role.name)
        new_rcg.move_roles(new_role_names)

    for new_rcg in new_service.get_all_role_config_groups():
        if new_rcg.roleType == "CATALOGSERVER":
            special_port_config_dict = { "catalogd_cmd_args_safety_valve" :
                                         "-state_store_subscriber_port=%s" % (saved_special_port,) }
            new_rcg.update_config(special_port_config_dict)
            print_configs("rcg " + new_rcg.displayName, special_port_config_dict)

def main():
    args = parse_args()
    api = ApiResource(args.host,
                      username=args.user,
                      password=args.password,
                      version=10)

    cluster = find_cluster(api, args.cluster)
    check_new_service_does_not_exist(api, cluster, args.service_name)
    if args.subparsers_name == "clone":
        template_service = find_template_service(api, cluster, args.based_on)
    else:
        master_host = find_master_host(api, args.host, args.master_host)
        deps = find_dependencies(args, cluster)

    parcel = get_best_parcel(api, cluster)
    if not parcel:
        ensure_parcel_repo_added(api)

        for attempt in xrange(1, MAX_PARCEL_REPO_WAIT_SECS + 1):
            parcel = get_best_parcel(api, cluster)
            if parcel:
                break
            print "Could not find parcel in attempt %d, will sleep and retry" % (attempt,)
            time.sleep(1)
        else:
            raise Exception("No parcel showed up in %d seconds" % (MAX_PARCEL_REPO_WAIT_SECS,))

    ensure_parcel_activated(cluster, parcel)
    if args.subparsers_name == "create":
        create_new_service(api, cluster, args.service_name, deps, args.scratch_dirs, master_host)
    else:
        clone_existing_service(cluster, args.service_name, template_service)

if __name__ == "__main__":
    main()
