#!/usr/bin/env python
"""SLURM toolkit for accounting analysis.

##############################################################################
#                                                                            #
# Copyright (c) 2013-2016, Yong Qin <yong.qin@lbl.gov>. All rights reserved. #
#                                                                            #
##############################################################################

This interface defines the components for SLURM accounting analysis.
"""


__author__ = "Yong Qin <yong.qin@lbl.gov>"
__date__ = "June 8, 2013"
__version__ = "0.1"


import ConfigParser
import re
from datetime import datetime, timedelta
from tools import *



# global variables
# storage units in bytes
KB = 1024.
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB
PB = 1024 * TB
EB = 1024 * PB

# time units in seconds
MINUTE = 60.
HOUR = 60 * MINUTE
DAY = 24 * HOUR

# job events
EVENT_SUBMIT   = 1
EVENT_ELIGIBLE = 2
EVENT_START    = 3
EVENT_END      = 4

# classes
class Node:
    """
    The Node class that maps to the NODE in SLURM.
    """


    def __init__(self, name, ppn, pd, pi, pp, modifier):
        """
        Default constructor.
        """
        self.data = {}
        self['Name']      = name
        self['PPN']       = ppn
        self['PowerDown'] = pd
        self['PowerIdle'] = pi
        self['PowerPeak'] = pp
        self['Modifier']  = modifier


    def __getitem__(self, key):
        """
        Returns self.data[key].
        """
        try:
            return self.data[key]
        except:
            raise KeyError('\"%s\" is an invalid Node field.' % key)


    def __setitem__(self, key, value):
        """
        Sets self.data[key].
        """
        try:
            self.data[key] = value
        except:
            raise KeyError('\"%s\" is an invalid Node field.' % key)



class Partition:
    """
    The Partition class that maps to the PARTITION in SLURM.
    """


    def __init__(self, name, nodelist, shared, modifier):
        """
        Default constructor.
        """
        self.data = {}
        self['Name']     = name
        self['Nodes']    = nodelist
        self['Shared']   = shared
        self['Modifier'] = modifier


    def __getitem__(self, key):
        """
        Returns self.data[key].
        """
        try:
            return self.data[key]
        except:
            raise KeyError('\"%s\" is an invalid Partition field.' % key)


    def __setitem__(self, key, value):
        """
        Sets self.data[key].
        """
        try:
            self.data[key] = value
        except:
            raise KeyError('\"%s\" is an invalid Partition field.' % key)



class QOS:
    """
    The QOS class that maps to the QOS in SLURM.
    """


    def __init__(self, name, modifier):
        """
        Default constructor.
        """
        self.data = {}
        self['Name']     = name
        self['Modifier'] = modifier


    def __getitem__(self, key):
        """
        Returns self.data[key].
        """
        try:
            return self.data[key]
        except:
            raise KeyError('\"%s\" is an invalid QOS field.' % key)


    def __setitem__(self, key, value):
        """
        Sets self.data[key].
        """
        try:
            self.data[key] = value
        except:
            raise KeyError('\"%s\" is an invalid QOS field.' % key)



class Account:
    """
    The Account class that maps to the ACCOUNT in SLURM.
    """


    def __init__(self, name, charstr, div, pi, pi_id, contact, modifier):
        """
        Default constructor.
        """
        self.data = {}
        self['Name']      = name
        self['ChargeStr'] = charstr
        self['Division']  = div
        self['PIName']    = pi.split('<')[0].strip()
        self['PIID']      = pi_id
        self['Contact']   = [x.strip() for x in pi.split(',') if x]
        self['Contact'].extend([x.strip() for x in contact.split(',') if x])
        self['Modifier']  = modifier


    def __getitem__(self, key):
        """
        Returns self.data[key].
        """
        try:
            return self.data[key]
        except:
            raise KeyError('\"%s\" is an invalid Account field.' % key)


    def __setitem__(self, key, value):
        """
        Sets self.data[key].
        """
        try:
            self.data[key] = value
        except:
            raise KeyError('\"%s\" is an invalid Account field.' % key)



class Cluster:
    """
    The Cluster class. Cluster class is a container class which includes
    instances for Nodes, Partitions, Accounts, Jobs and JobStats. It could be a 
    real CLUSTER in SLURM, or a METACLUSTER with pre-defined PARTITIONS.
    """


    def __init__(self, config_file, config_cluster='', start=None, end=None, \
        sacct_opt='', sacct_file=''):
        """
        Default constructor.
        """
        self.data = {}
        self['Cluster']    = ''
        self['Partitions'] = {}
        self['QOSs']       = {}
        self['Nodes']      = {}
        self['Accounts']   = {}
        self['Steps']      = []
        self['Jobs']       = []
        self['JobStats']   = {}
        self['Start']      = None
        self['End']        = None
        self['NNodes']     = 0
        self['NCPUs']      = 0
        self['ServUnits']  = 0
        self['CPUTime']    = 0
        self['PowerDown']  = 0
        self['PowerIdle']  = 0
        self['PowerPeak']  = 0

        if type(start) is datetime:
            self['Start'] = start
        else:
            self['Start'] = datetime.now()

        if type(end) is datetime:
            self['End'] = end
        else:
            self['End'] = datetime.now()

        self.ParseConfig(config_file, config_cluster)

        self['NNodes'] = len(self['Nodes'])

        self['NCPUs'] = sum([self['Nodes'][x]['PPN'] for x in self['Nodes']])

        # TODO: Need to consider DST.
        self['ServUnits'] = total_seconds(self['End'] - self['Start']) * \
            sum([self['Nodes'][x]['PPN'] * self['Nodes'][x]['Modifier'] for x \
            in self['Nodes']])

        # TODO: Need to consider DST.
        self['CPUTime'] = total_seconds(self['End'] - self['Start']) * \
            self['NCPUs']

        self['PowerDown'] = sum([self['Nodes'][x]['PowerDown'] for x in \
            self['Nodes']])

        self['PowerIdle'] = sum([self['Nodes'][x]['PowerIdle'] for x in \
            self['Nodes']])

        self['PowerPeak'] = sum([self['Nodes'][x]['PowerPeak'] for x in \
            self['Nodes']])

        # Reset JobStep.Fields and Job.Fields to match the current version of 
        # SLURM.
        JobStep.GetFields()
        Job.AddFields()

        self.CollectSteps(sacct_opt, sacct_file)
        self.CollectJobs()
        self.PurgeJobs()
        self.CollectStats()
        debug(self['JobStats']['Account'])


    def __getitem__(self, key):
        """
        Returns self.data[key].
        """
        try:
            return self.data[key]
        except:
            raise KeyError('\"%s\" is an invalid Cluster field.' % key)


    def __setitem__(self, key, value):
        """
        Sets self.data[key].
        """
        try:
            self.data[key] = value
        except:
            raise KeyError('\"%s\" is an invalid Cluster field.' % key)


    def ParseConfig(self, filename, cluster=''):
        """
        Parses config file to obtain cluster configurations.
        """
        info('Start parsing configs.')

        assert_file(filename)

        config = ConfigParser.SafeConfigParser()
        config.read(filename)

        if not cluster:
            cluster = config.sections()

            if len(cluster) > 1:
                raise ConfigParser.Error( \
                    'More than 1 configured cluster in \"%s\".' % filename)
            else:
                cluster = config.sections()[0]

        if not config.has_section(cluster):
            raise ConfigParser.NoSectionError( \
                'Cluster \"%s\" is not defined in \"%s\".' % (cluster, \
                filename))

        self['Cluster'] = cluster

        # Partitions.
        partitions = [x[1].strip() for x in config.items(cluster) if \
            x[0].startswith('partition')]

        if not partitions:
            raise ConfigParser.NoOptionError( \
                'Missing partitions definition in \"%s\".' % filename)

        qoss = [x[1].strip() for x in config.items(cluster) if \
            x[0].startswith('qos')]

        if not qoss:
            raise ConfigParse.NoOptionError( \
                'Missing QOSs definition in \"%s\".' % filename)

        # Nodes.
        nodes = [x[1].strip() for x in config.items(cluster) if \
            x[0].startswith('nodes')]

        if not nodes:
            raise ConfigParser.NoOptionError( \
                'Missing nodes definition in \"%s\".' % filename)

        # Accounts.
        accounts = [x[1].strip() for x in config.items(cluster) if \
            x[0].startswith('account')]

        if not accounts:
            raise ConfigParser.NoOptionError( \
                'Missing accounts definition in \"%s\".' % filename)

        self.NormPartitions(partitions)
        self.NormQOSs(qoss)
        self.NormNodes(nodes)
        self.NormAccounts(accounts)

        debug('Read RAW partitions \"%s\".' % sorted(self['Partitions'].keys()))
        debug('Read RAW QoSs \"%s\".' % sorted(self['QOSs'].keys()))
        debug('Read RAW nodes \"%s\".' % sorted(self['Nodes'].keys()))
        debug('Read RAW accounts \"%s\".' % sorted(self['Accounts'].keys()))

        info('End parsing configs.')


    def NormPartitions(self, partstrlst):
        """
        Normalizes partition definitions for this cluster. "partstr" is defined 
        as "partition:node_list:shared:charge_modifier", e.g., 
        hadley:n[0000-0016].hadley0:SHARED:0.00
        """
        for partstr in partstrlst:
            tmp = [x.strip() for x in partstr.split(':')]

            part     = tmp[0]
            modifier = float(tmp[3])

            if tmp[2].lower() == 'shared':
                shared = True
            else:
                shared = False

            # Expands node range.
            nodelist = [x.strip() for x in re.sub( \
                r'([\w\-]*)\[([\d\s\-\,]+)\]([\.\w]*)', exp_rangestr, \
                tmp[1] ).split(',')]

            self['Partitions'][part] = Partition(part, nodelist, shared, \
                modifier)


    def NormQOSs(self, qosstrlst):
        """
        Normalizes QOSs for this cluster. "qosstrlst" is defined as
        "QOS:charge_modifier", e.g., "normal:0.01"
        """
        for qosstr in qosstrlst:
            tmp = [x.strip() for x in qosstr.split(':')]

            qos = tmp[0]
            modifier = float(tmp[1])

            self['QOSs'][qos] = QOS(qos, modifier)


    def NormNodes(self, nodestrlst):
        """
        Normalizes node definitions for this cluster. "nodestr" is defined 
        as "node_list:ppn:power_halt:power_idle:power_peak:charge_modifier", 
        e.g., n[0000-0016].hadley0:8:0:0:0:1.00
        """
        for nodestr in nodestrlst:
            tmp = [x.strip() for x in nodestr.split(':')]

            ppn        = int(tmp[1])
            power_down = float(tmp[2])
            power_idle = float(tmp[3])
            power_peak = float(tmp[4])
            modifier   = float(tmp[5])

            # Expands node range.
            nodelist = exp_noderangestr(tmp[0])

            for node in nodelist:
                self['Nodes'][node] = Node(node, ppn, power_down, power_idle, \
                    power_peak, modifier)


    def NormAccounts(self, acctstrlst):
        """
        Normalizes account definitions for this cluster. "acctstr" is defined
        as "account:charge_string:division:PI:PI_ID:contact:charge_modifier", 
        e.g., hadley::hadley:John Chiang <jchiang@atmos.berkeley.edu>::Thomas 
        Powell <zackp@berkeley.edu>:0.00
        """
        for acctstr in acctstrlst:
            tmp = [x.strip() for x in acctstr.split(':')]

            acct     = tmp[0]
            charstr  = tmp[1]
            div      = tmp[2]
            pi       = tmp[3]
            pi_id    = tmp[4]
            contact  = tmp[5]
            modifier = float(tmp[6])

            self['Accounts'][acct] = Account(acct, charstr, div, pi, pi_id, \
                contact, modifier)


    def CollectStepsFromString(self, var):
        """
        Collects job steps from string.
        """
        info('Start collecting job steps from strings.')

        steps = [step for step in var.split('\n') if step]
        for step in steps:
            tmp = JobStep.FromString(step)
            if tmp:
                # Overrides the default cluster.
                tmp['Cluster'] = self['Cluster']
                self['Steps'].append(tmp)
                debug('Read RAW JobStep \"%s\"' % tmp['JobID'])

        info('End collecting job steps from strings.')


    def CollectStepsFromSacct(self, opts):
        """
        Collects job steps from sacct command for completed jobs.
        """
        # Default 'sacct' command line options.
        opt_ext = ' -a -n -D -P -s R -o ' + ','.join(JobStep.Fields)
        opt_ext += ' -S %s' % self['Start'].strftime('%Y-%m-%dT%H:%M:%S')
        opt_ext += ' -E %s' % self['End'].strftime('%Y-%m-%dT%H:%M:%S')

        # Populating options to command line.
        # Accounts
        if '-A' in opts:
            opt_ext += ' -A %s' % opts['-A']
        else:
            opt_ext += ' -A %s' % ','.join(self['Accounts'])

        # Partitions
        if '-r' in opts:
            opt_ext += ' -r %s' % opts['-r']
        else:
            opt_ext += ' -r %s' % ','.join(self['Partitions'])

        # QOSs
        if '-q' in opts:
            opt_ext += ' -q %s' % opts['-q']
        else:
            opt_ext += ' -q %s' % ','.join(self['QOSs'])

        # The rest of opts.
        for opt in opts:
            if opt in ['-g']:
                opt_ext += ' -g %s' % opts[opt]
            elif opt in ['-j']:
                opt_ext += ' -j %s' % opts[opt]
            elif opt in ['-N']:
                opt_ext += ' -N %s' % opts[opt]
            elif opt in ['-u']:
                opt_ext += ' -u %s' % opts[opt]

        # Complete command line and run it.
        cmd = 'sacct' + opt_ext
        debug(cmd)
        info('Start running sacct command.')
        output = run_cmd(cmd)
        info('End running sacct command.')

        if output['retval']:
            raise RuntimeError('Failed to run \"%s\", error \"%s\".' % \
                (cmd, output['retstr'].strip()))

        self.CollectStepsFromString(output['retstr'])


    def CollectStepsFromFile(self, filename):
        """
        Collects job steps from a file output from sacct.
        """
        output = read_file(filename)
        self.CollectStepsFromString(output)


    def CollectSteps(self, sacct_opts={}, sacct_file=''):
        """
        Collect job steps from a provided sacct command, or a sacct output file.
        """
        info('Start collecting job steps.')

        if sacct_file:
            self.CollectStepsFromFile(sacct_file)
        else:
            self.CollectStepsFromSacct(sacct_opts)

        info('End collecting job steps.')


    def CollectJobs(self):
        """
        Collects jobs from job steps.
        """
        info('Start collecting jobs.')

        # JobIDs to track all Jobs.
        jobids = []

        # Integrates all job steps together.
        for step in self['Steps']:
            jobid = step['JobID'].split('.')[0]

            # This JobStep initiates a new Job.
            if jobid == step['JobID']:
                debug('Initialize Job \"%s\"' % jobid)
                job = Job()
                job.AddStep(step, self['Partitions'], self['QOSs'], \
                    self['Nodes'], self['Accounts'])
                self['Jobs'].append(job)
                debug('Read JobStep \"%s\"' % step['JobID'])

                jobids.append(jobid)

            # This JobStep belongs to the same Job as last JobStep.
            elif jobid in jobids and jobid == self['Jobs'][-1]['JobID']:
                self['Jobs'][-1].AddStep(step, self['Partitions'], \
                    self['QOSs'], self['Nodes'], self['Accounts'])
                debug('Read JobStep \"%s\"' % step['JobID'])

            # This JobStep doesn't follow the current Job but belongs a Job 
            # that's previously scanned. When this happens, it means
            # out-of-order list has been given.
            elif jobid in jobids:
                warning('JobStep \"%s\" does not follow current Job, ignore.' % \
                    step['JobID'])
                # TODO: Need to be able to handle job steps that are out of 
                #       orders.
                pass

            # This is a new JobStep that doesn't have a parent at all.
            else:
                warning('JobStep \"%s\" does not have a parent, ignore.' % \
                    step['JobID'])
                pass

        info('End collecting jobs.')


    def PurgeJobs(self):
        """
        Removes jobs that do not belong to the defined cluster config.
        """
        info('Start purging jobs.')

        # TODO: remove() is too heavy, another idea is to store the indices in
        # an array and copy the rest of self['Jobs'] to another list
        for job in reversed(self['Jobs']):

            if job['Partition'] not in self['Partitions']:
                debug('Purge job \"%s\" with undesignated partition - \"%s\".' \
                    % (job['JobID'], job['Partition']))
                self['Jobs'].remove(job)

            elif job['Account'] not in self['Accounts']:
                debug('Purge job \"%s\" with undesignated account - \"%s\".' \
                    % (job['JobID'], job['Account']))
                self['Jobs'].remove(job)

            elif not set(job['NodeList']).issubset(set(self['Nodes'].keys())):
                debug('Purge job \"%s\" with undesignated nodes - \"%s\".' \
                    % (job['JobID'], job['NodeList']))
                self['Jobs'].remove(job)

            # sacct often over reports jobs unless '-s R' is used. The following
            # logic will remove those that are not defined in the time range.
            # TODO: Need to consider DST, e.g., start: 1:50am, end: 1:20am
            elif job['End'] < self['Start'] or job['End'] > self['End']:
                debug('Purge job \"%s\" not within time range.' % job['JobID'])
                self['Jobs'].remove(job)

        info('End purging jobs.')


    def CollectStats(self):
        """
        Generates job stats for this cluster.
        """
        info('Start generating job stats.')

        self['JobStats'] = JobStats(self['Jobs'], self['ServUnits'], \
            self['CPUTime'])

        info('End generating job stats.')


    def ShowSummary(self, groups, orderby='', reverse=False, format='text',
        table=False, verbose=False):
        """
        Show job stats summary for this cluster.
        """
        buf = ''
        comprehensive = False

        if verbose > 2:
            comprehensive = True

        if format == 'text' and not table:
            buf += self['JobStats'].SummaryText(self['Start'], self['End'], \
                self['CPUTime'], self['ServUnits'], groups, orderby=orderby, \
                reverse=reverse, comprehensive=comprehensive)

        elif format == 'text' and table:
            buf += self['JobStats'].SummaryTextTable(self['Start'], \
                self['End'], self['CPUTime'], self['ServUnits'], \
                groups, orderby=orderby, reverse=reverse)

        elif format == 'html' and not table:
            buf += self['JobStats'].SummaryHtml(self['Start'], self['End'], \
                self['CPUTime'], self['ServUnits'], groups, orderby=orderby, \
                reverse=reverse)

        elif format =='html' and table:
            buf += self['JobStats'].SummaryHtmlTable(self['Start'], \
                self['End'], self['CPUTime'], self['ServUnits'], \
                self['NNodes'], self['NCPUs'], self['PowerIdle'], \
                self['PowerPeak'], self['Jobs'], groups, orderby=orderby, \
                reverse=reverse)

        return buf


    def ShowDetail(self, format='text', table=False, verbose=False):
        """
        Show job details for this cluster.
        """
        buf = ''

        if format == 'text' and not table:
            for job in self['Jobs']:
                buf += job.ToText(indent='', delimiter=': ', title=True, \
                    verbose=verbose)
                buf += '\n'

        elif format == 'text' and table:
            pass

        elif format == 'html' and not table:
            pass

        elif format == 'html' and table:
            pass

        return buf



class JobStep:
    """
    The Job Step class for SLURM jobs.

    Variables:
        JobStep.Fields defines all SLURM resource fields that 'sacct' provides.
    """


    #
    # JobStep.Fields defines all SLURM resource fields that 'sacct' provides.
    # These fields can be reset by JobStep.GetFields() class method to reflect
    # all the fields that current version of SLURM provids.
    #
    # Note: 
    #     CPUTime = Elapsed * AllocCPUS = Used CPU Time
    #     Elapsed = Used Wall Clock Time
    #     Reserved = Queue Wait Time
    #     TimeLimit = Requested Wall Clock Time
    #     TotalCPU = SystemCPU + UserCPU
    #

    Fields = []
    #Fields = ['AllocCPUS', 'Account', 'AssocID', 'AveCPU', 'AveCPUFreq', \
    #    'AvePages', 'AveRSS', 'AveVMSize', 'BlockID', 'Cluster', 'Comment', \
    #    'ConsumedEnergy', 'CPUTime', 'CPUTimeRAW', 'DerivedExitCode', \
    #    'Elapsed', 'Eligible', 'End', 'ExitCode', 'GID', 'Group', 'JobID', 
    #    'JobName', 'Layout', 'MaxPages', 'MaxPagesNode', 'MaxPagesTask', \
    #    'MaxRSS', 'MaxRSSNode', 'MaxRSSTask', 'MaxVMSize', 'MaxVMSizeNode', \
    #    'MaxVMSizeTask', 'MinCPU', 'MinCPUNode', 'MinCPUTask', 'NCPUS', \
    #    'NNodes', 'NodeList', 'NTasks', 'Priority', 'Partition', 'QOS', \
    #    'QOSRAW', 'ReqCPUS', 'Reserved', 'ResvCPU', 'ResvCPURAW', 'Start', \
    #    'State', 'Submit', 'Suspended', 'SystemCPU', 'Timelimit', 'TotalCPU', \
    #    'UID', 'User', 'UserCPU', 'WCKey', 'WCKeyID']


    def __init__(self):
        """
        Default constructor.
        """
        self.data = {}


    def __getitem__(self, key):
        """
        Returns self.data[key].
        """
        try:
            return self.data[key]
        except:
            raise KeyError('\"%s\" is an invalid JobStep field.' % key)


    def __setitem__(self, key, value):
        """
        Sets self.data[key].
        """
        try:
            self.data[key] = value
        except:
            raise KeyError('\"%s\" is an invalid JobStep field.' % key)


    def __str__(self):
        """
        Returns the string representation of the instance.
        """
        return self['JobID']


    @classmethod
    def GetFields(cls):
        """
        Collects JobStep.Fields from "sacct -e" command output.
        """
        cmd = 'sacct -e'
        output = run_cmd(cmd)
        JobStep.Fields = output['retstr'].split()


    @classmethod
    def FromList(cls, var):
        """
        Constructor that instantiates from a list.
        """
        if type(var) is not list:
            raise TypeError('\"%s\" is not a List.' % var)

        if len(cls.Fields) != len(var):
            warning('\"%s\" (%d) does not have the same size as a JobStep (%d), ignore.' % \
                (var, len(var), len(cls.Fields)))
            return None

        step = cls()

        for i in range(len(cls.Fields)):
            step[cls.Fields[i]] = var[i]

        # Ignores running and pending jobs.
        if step['State'] in ['RUNNING', 'PENDING']:
            return None

        else:
            step.PostInit()
            return step


    @classmethod
    def FromString(cls, var):
        """
        Constructor that instantiates from a string.
        """
        if type(var) is not str:
            raise TypeError('\"%s\" is not a String.' % var)

        step = cls.FromList(var.split('|'))

        return step


    @staticmethod
    def Validate(step):
        """
        Validates if step contains all fields or not.
        """
        return set(step.data.keys()) == set(JobStep.Fields)


    def PostInit(self):
        """
        Does some post-initialization work, such as normalizing data, feeding 
        extra fields, etc.
        """
        for key in JobStep.Fields:

            # timestamps
            if key in ['Eligible', 'End', 'Start', 'Submit']:
                self[key] = norm_timestamp(self[key])

            # time values
            elif key in ['CPUTime', 'Elapsed', 'MinCPU', 'Reserved', \
                'ResvCPU', 'Suspended', 'SystemCPU', 'Timelimit', 'TotalCPU', \
                'UserCPU']:
                self[key] = norm_timelapse(self[key])

            # integer values
            elif key in ['AllocCPUS', 'CPUTimeRAW', 'NCPUS', 'NNodes', \
                'ReqCPUS', 'ResvCPURAW']:

                try:
                    self[key] = int(self[key])
                except:
                    pass

            # storage fields can also be converted to numbers
            elif key in ['AveDiskRead', 'AveDiskWrite', 'AvePages', 'AveRSS', \
                'AveVMSize', 'MaxDiskRead', 'MaxDiskWrite', 'MaxPages', \
                'MaxRSS', 'MaxVMSize']:

                if not self[key]:
                    pass

                elif self[key].endswith('K'):
                    self[key] = float(self[key].strip('K')) * KB

                elif self[key].endswith('M'):
                    self[key] = float(self[key].strip('M')) * MB

                elif self[key].endswith('G'):
                    self[key] = float(self[key].strip('G')) * GB

                elif self[key].endswith('T'):
                    self[key] = float(self[key].strip('T')) * TB

                elif self[key].endswith('P'):
                    self[key] = float(self[key].strip('P')) * PB

                elif self[key].endswith('E'):
                    self[key] = float(self[key].strip('E')) * EB

                else:
                    self[key] = float(self[key])

            elif key in ['ConsumedEnergy', 'ConsumedEnergyRaw']:
                if self[key]:
                    self[key] = float(self[key])

            elif key in ['NodeList']:
                self[key] = exp_noderangestr(self[key])
                # TODO: need to be able to handle hostname changes, e.g.,
                # self[key] = [each.replace('savio0', 'savio1') for each in self[key]]


    def ToList(self):
        """
        Converts job step record to list.
        """
        return [self[key] for key in JobStep.Fields]


    def ToText(self, indent='', delimiter=': ', title=False):
        """
        Converts job step record to text format.
        """
        buf = ''

        if title:
            buf += indent + 'StepID: %s\n' % (self['JobID'])

        for key in sorted(JobStep.Fields):
            if key not in ['Eligible', 'End', 'Start', 'Submit']:
                buf += '%s%s%s%s\n' % (' '*2+indent, key, delimiter, self[key])
            else:
                buf += '%s%s%s%s\n' % (' '*2+indent, key, delimiter, \
                    self[key].strftime('%Y-%m-%d %H:%M:%S'))

        return buf


    def TableHeader(self):
        """
        Outputs the header of the step record.
        """
        pass


    def TableDivider(self):
        """
        Outputs the divider line of the step record.
        """
        pass


    def TableRow(self):
        """
        Converts job step record to a row of table.
        """
        pass


    @staticmethod
    def ListSteps(steps):
        """
        Lists steps (a step list) in tabular format.
        """
        pass


    def ToHTML(self):
        """
        Converts job step record to HTML format.
        """
        pass


    def HTMLHeader(self):
        """
        Outputs the header to HTML format.
        """
        pass


    def ToHTMLRow(self):
        """
        Converts job step record to a row of HTML table.
        """
        pass



class Job:
    """
    The Job class for SLURM jobs.

    Variables:
        Job.Fields defines the Job attributes that needs to be collected.
    """


    # Job.Fields defines the Job attributes that needs to be collected.
    # Notes:
    #     EfficiencyT = TotalCPU / NodeTime
    #     EfficiencyU = UserCPU / TotalCPU
    Fields = ['JobID', 'JobName', 'ServUnits', 'Charge', 'User', 'Account', \
        'Group', 'Division', 'QOS', 'Partition', 'Cluster', 'Submit', \
        'Eligible', 'Start', 'End', 'State', 'ExitCode', 'ReqCPUS', 'NSteps', \
        'AllocCPUS', 'NNodes', 'NCPUS', 'Timelimit', 'Reserved', 'Elapsed', \
        'TotalCPU', 'SystemCPU', 'UserCPU', 'NodeTime', 'CPUTime', \
        'EfficiencyT', 'EfficiencyU', 'MaxRSS', 'MaxRSSNode', 'AveRSS', \
        'MaxVMSize', 'MaxVMSizeNode', 'AveVMSize', 'ConsumedEnergy', 'NodeList']


    def __init__(self):
        """
        Default constructor.
        """
        self.data  = {}
        self.steps = []


    def __getitem__(self, key):
        """
        Returns self.data[key].
        """
        if key in Job.Fields:
            return self.data[key]
        else:
            raise KeyError('\"%s\" is an invalid Job field.' % key)


    def __setitem__(self, key, value):
        """
        Sets self.data[key].
        """
        if key in Job.Fields:
            self.data[key] = value
        else:
            raise KeyError('\"%s\" is an invalid Job field.' % key)


    def __str__(self):
        """
        Returns the string representation of the instance.
        """
        return self['JobID']


    @classmethod
    def AddFields(cls):
        """
        Adds new fields if they are not in the current ones.
        """
        fields = ['AveDiskRead', 'AveDiskWrite', 'MaxDiskRead', \
            'MaxDiskWrite', 'MaxDiskReadNode', 'MaxDiskWriteNode', \
            'ReqMem', 'ReqCPUFreq']

        for f in fields:
            if f in JobStep.Fields and f not in Job.Fields:
                Job.Fields.append(f)


    def AddStep(self, step, parts_conf, qoss_conf, nodes_conf, accts_conf):
        """
        Adds a job step to this job.
        """
        # Add first JobStep.
        if 'JobID' not in self.data:
            self['JobID'] = step['JobID'].split('.')[0]
            self.steps.append(step)

        else:
            if self['JobID'] == step['JobID'].split('.')[0]:

                # If this JobID is the same as an existing one (duplicate), do
                # some sanity check, and then add the JobStep.
                if self['JobID'] == step['JobID']:
                    if self['JobName'] != step['JobName'] or \
                        self['User'] != step['User'] or \
                        self['Partition'] != step['Partition']:
                        raise ValueError( \
                            'Duplicate job \"%s\" does not match.' % \
                            self['JobID'])
                    else:
                        warning('Duplicate job \"%s\" detected.' % \
                            self['JobID'])

                self.steps.append(step)

            else:
                raise ValueError('Step \"%s\" does not match Job \"%s\"' % \
                    (step['JobID'], self['JobID']))

        self.PopData(step, parts_conf, qoss_conf, nodes_conf, accts_conf)


    def PopData(self, step, parts_conf, qoss_conf, nodes_conf, accts_conf):
        """
        Populates data from a step to a job.
        """
        # Normal fields can be taken from the job step directly.
        if self['JobID'] == step['JobID']:

            # Populates common fields first.
            for key in Job.Fields:
                if key in JobStep.Fields:
                    self[key] = step[key]

            # Number of steps.
            self['NSteps'] = 0

            # Job ran on these CPUs.
            try:
                cpulist = exp_cpulist(self, nodes_conf, \
                    parts_conf[self['Partition']]['Shared'])
            except KeyError:
                warning('Job \"%s\" has an undefined partition \"%s\", ignore.' % \
                    (self['JobID'], self['Partition']))
                cpulist = []

            # Extended fields that are not in the job step.
            try:
                self['Division'] = accts_conf[self['Account']]['Division']
            except KeyError:
                self['Division'] = 'UNKNOWN'

            try:
                self['NodeTime'] = timedelta(seconds=total_seconds( \
                    self['Elapsed']) * len(cpulist))
                self['ServUnits'] = timedelta(seconds=total_seconds( \
                    self['Elapsed']) * sum(nodes_conf[node]['Modifier'] for \
                    node in cpulist))
            except KeyError:
                self['NodeTime'] = timedelta(0)
                self['ServUnits'] = timedelta(0)

            try:
                self['Charge'] = total_seconds(self['ServUnits']) / HOUR * \
                    parts_conf[self['Partition']]['Modifier'] * \
                    qoss_conf[self['QOS']]['Modifier'] * \
                    accts_conf[self['Account']]['Modifier']
            except KeyError:
                self['Charge'] = 0.

            try:
                self['EfficiencyT'] = \
                    total_seconds(self['TotalCPU']) / \
                    total_seconds(self['NodeTime'])

                if self['EfficiencyT'] > 1.0:
                    warning('EfficiencyT (%g) of job \"%s\" greater than 1.' % \
                        (self['EfficiencyT'], self['JobID']))
            except ZeroDivisionError:
                self['EfficiencyT'] = 0.

            try:
                self['EfficiencyU'] = \
                    total_seconds(self['UserCPU']) / \
                    total_seconds(self['TotalCPU'])

                if self['EfficiencyU'] > 1.0:
                    warning('EfficiencyU (%g) of job \"%s\" greater than 1.' % \
                        (self['EfficiencyU'], self['JobID']))
            except ZeroDivisionError:
                self['EfficiencyU'] = 0.

            # Storage usage fields.
            for key in ['AveDiskRead', 'AveDiskWrite', 'AveRSS', 'AveVMSize', \
                'MaxDiskRead', 'MaxDiskWrite', 'MaxRSS', 'MaxVMSize']:
                if key in JobStep.Fields and type(self[key]) is not float:
                    self[key] = 0.

            # Power consumption.
            for key in ['ConsumedEnergy']:
                if self[key] is float and self[key] > 0:
                    warning('Job \"%s\" has a reported ConsumedEnergy field, ignore.' \
                        % self['JobID'])

            self['ConsumedEnergy'] = sum((nodes_conf[node]['PowerPeak'] - \
                nodes_conf[node]['PowerIdle']) / nodes_conf[node]['PPN'] * \
                self['EfficiencyT'] for node in cpulist)

        else:
            # Number of steps.
            self['NSteps'] += 1

            # Fields need to be populated from all steps.
            for key in ['AveDiskRead', 'AveDiskWrite', 'AveRSS', 'AveVMSize']:
                if key in JobStep.Fields and cmp_float(step[key], self[key]):
                    self[key] = step[key]

            # TODO: need to split them out
            if 'MaxDiskRead' in JobStep.Fields and \
                cmp_float(step['MaxDiskRead'], self['MaxDiskRead']):
                self['MaxDiskRead']     = step['MaxDiskRead']
                self['MaxDiskReadNode'] = step['MaxDiskReadNode']

            if 'MaxDiskWrite' in JobStep.Fields and \
                cmp_float(step['MaxDiskWrite'], self['MaxDiskWrite']):
                self['MaxDiskWrite']     = step['MaxDiskWrite']
                self['MaxDiskWriteNode'] = step['MaxDiskWriteNode']

            if cmp_float(step['MaxRSS'], self['MaxRSS']):
                self['MaxRSS']     = step['MaxRSS']
                self['MaxRSSNode'] = step['MaxRSSNode']

            if cmp_float(step['MaxVMSize'], self['MaxVMSize']):
                self['MaxVMSize']     = step['MaxVMSize']
                self['MaxVMSizeNode'] = step['MaxVMSizeNode']

            # Warn about feeding power consumption values.
            for key in ['ConsumedEnergy']:
                if step[key] is float and step[key] > 0:
                    warning('Step \"%s\" has a reported ConsumedEnergy field, ignore.' \
                        % step['JobID'])


    @staticmethod
    def Validate(job):
        """
        Validates if job contains all fields or not.
        """
        if set(job.data.keys()) != set(Job.Fields):
            return False

        for key in Job.Fields:

            # integers
            if key in ['AllocCPUS', 'NCPUS', 'NNodes', 'ReqCPUS']:
                if type(step[key]) is not int:
                    raise TypeError('Job \"%s\" has an invalid \"%s\" field.' \
                        % (step['JobID'], key))

            # floats
            elif key in ['AveRSS', 'AveVMSize', 'Charge', 'EfficiencyT', \
                'EfficiencyU', 'MaxRSS', 'MaxVMSize', 'ServUnits']:
                if type(step[key]) is not float:
                    raise TypeError('Job \"%s\" has an invalid \"%s\" field.' \
                        % (step['JobID'], key))

            # strings
            elif key in ['Account', 'Cluster', 'Division', 'ExitCode', \
                'Group', 'JobID', 'JobName', 'MaxRSSNode', 'MaxVMSizeNode', \
                'NodeList', 'Partition', 'QOS', 'User']:
                if type(step[key]) is not str:
                    raise TypeError('Job \"%s\" has an invalid \"%s\" field.' \
                        % (step['JobID'], key))

            # datetime.timedelta
            elif key in ['CPUTime', 'Elapsed', 'Reserved', 'NodeTime', \
                'SystemCPU', 'Timelimit', 'TotalCPU', 'UserCPU']:
                if type(step[key]) is not timedelta:
                    raise TypeError('Job \"%s\" has an invalid \"%s\" field.' \
                        % (step['JobID'], key))

            # datetime.datetime
            elif key in ['Eligible', 'End', 'Start', 'Submit']:
                if type(step[key]) is not datetime:
                    raise TypeError('Job \"%s\" has an invalid \"%s\" field.' \
                        % (step['JobID'], key))


    def ToList(self):
        """
        Converts job record to list.
        """
        return [self[key] for key in Job.Fields]


    def ToText(self, indent='', delimiter=': ', title=False, verbose=False):
        """
        Converts job record to text format.
        """
        buf = ''

        if title:
            buf += indent + 'JobID: %s\n' % (self['JobID'])

        for key in sorted(Job.Fields):

            if key not in ['Eligible', 'End', 'Start', 'Submit']:
                buf += '%s%s%s%s\n' % (' '*2+indent, key, delimiter, self[key])
            else:
                buf += '%s%s%s%s\n' % (' '*2+indent, key, delimiter, \
                    self[key].strftime('%Y-%m-%d %H:%M:%S'))

        if verbose:
            for step in self.steps:
                buf += '\n'
                buf += step.ToText(indent=' '*2+indent, delimiter=delimiter, \
                    title=title)

        return buf


    def TableRow(self):
        """
        Converts job record to a row of table.
        """
        pass


    @staticmethod
    def ListJobs(jobs):
        """
        Lists jobs (a job list) in tabular format.
        """
        pass


    def ToHTML(self):
        """
        Converts job record to HTML format.
        """
        pass


    def ToHTMLRow(self):
        """
        Converts job record to a row of HTML table.
        """
        pass



class JobStats:
    """
    The JobStats class for job statistics.

    Variables:
        JobStats.GroupBase defines the groups that are in common with
            Job.Fields.
        JobStats.GroupExt defines extra groups that requires special 
            handling.
        JobStats.GroupSum defines the groups used in the default summary report.
        JobStats.Resources defines all resource fields that need to be
            collected.
        JobStats.ResDef defines the group explanations and format strings.
    """


    # JobStats.GroupBase defines the groups that are in common with Job.Fields.
    GroupBase = ['Cluster', 'Partition', 'QOS', 'Division', 'Group', \
        'Account', 'User', 'State', 'ExitCode', 'JobID']

    # JobStats.GroupExt defines extra groups that requires special handling.
    GroupExt = ['SubmitYear', 'SubmitMonth', 'SubmitDay', 'SubmitHour', \
        'SubmitDate', 'SubmitWeekday', 'SubmitWeek', 'EndYear', 'EndMonth', \
        'EndDay', 'EndHour', 'EndDate', 'EndWeekday', 'EndWeek']
    GroupExt1 = ['Submit', 'End']
    GroupExt2 = ['Year', 'Month', 'Day', 'Hour', 'Date', 'Weekday', 'Week']

    # TODO: Node stats

    # JobStats.GroupSum defines the groups used in the default summary report.
    GroupSum = ['Cluster', 'Partition', 'QOS', 'Division', 'Account', 'User', \
        'State', 'ExitCode', 'SubmitDate', 'EndDate', 'SubmitWeekday', \
        'EndWeekday', 'SubmitHour', 'EndHour']

    # JobStats.Resources defines all resource fields that need to be collected.
    Resources = ['NumJobs', 'Charge', 'ServUnits', 'SUPer', 'NodeTime', \
        'NTPer', 'CPUTime', 'CTPer', 'TotalCPU', 'SystemCPU', 'UserCPU', \
        'EfficiencyT', 'EfficiencyU', 'Timelimit', 'Elapsed', 'Reserved', \
        'ReqCPUS', 'AllocCPUS', 'AveDiskRead', 'AveDiskWrite', 'AveRSS', \
        'AveVMSize', 'NSteps', 'JobID', 'JobIndex']

    # JobStats.ResDef defines the group explanations and format strings.
    ResDef = { \
        'NumJobs'     : ['Total Number of Jobs           ', '8d',    'NumJobs',              'd',   'Total Number of Jobs'           ], \
        'Charge'      : ['Total Charge                ($)', '11.2f', 'Charge ($)',           '.2f', 'Total Charge ($)'               ], \
        'ServUnits'   : ['Total Service Units       (hrs)', '11.2f', 'ServUnits (hrs)',      '.2f', 'Total Service Units (hrs)'      ], \
        'SUPer'       : ['Total Service Units         (%)', '11.2f', 'ServUnits (%)',        '.2f', 'Total Service Units (%)'        ], \
        'NodeTime'    : ['Total Node Time           (hrs)', '11.2f', 'NodeTime (hrs)',       '.2f', 'Total Node Time (hrs)'          ], \
        'NTPer'       : ['Total Node Time             (%)', '11.2f', 'NodeTime (%)',         '.2f', 'Total Node Time (%)'            ], \
        'CPUTime'     : ['Total CPU Time            (hrs)', '11.2f', 'CPUTime (hrs)',        '.2f', 'Total CPU Time (hrs)'           ], \
        'CTPer'       : ['Total CPU Time              (%)', '11.2f', 'CPUTime (%)',          '.2f', 'Total CPU Time (%)'             ], \
        'TotalCPU'    : ['Mean Total CPU Time       (hrs)', '11.2f', 'M.TotalTime (hrs)',    '.2f', 'Mean Total CPU Time (hrs)'      ], \
        'SystemCPU'   : ['Mean System CPU Time      (hrs)', '11.2f', 'M.SysTime (hrs)',      '.2f', 'Mean System CPU Time (hrs)'     ], \
        'UserCPU'     : ['Mean User CPU Time        (hrs)', '11.2f', 'M.UserTime (hrs)',     '.2f', 'Mean User CPU Time (hrs)'       ], \
        'EfficiencyT' : ['Mean Job Efficiency         (%)', '11.2f', 'M.Eff.T (%)',          '.2f', 'Mean Job Efficiency (%)'        ], \
        'EfficiencyU' : ['Mean User CPU Efficiency    (%)', '11.2f', 'M.Eff.U (%)',          '.2f', 'Mean User CPU Efficiency (%)'   ], \
        'Timelimit'   : ['Mean Req. Wall Clock Time (hrs)', '11.2f', 'M.ReqClockTime (hrs)', '.2f', 'Mean Req. Wall Clock Time (hrs)'], \
        'Elapsed'     : ['Mean Wall Clock Time      (hrs)', '11.2f', 'M.ClockTime (hrs)',    '.2f', 'Mean Wall Clock Time (hrs)'     ], \
        'Reserved'    : ['Mean Queue Wait Time      (hrs)', '11.2f', 'M.WaitTime (hrs)',     '.2f', 'Mean Queue Wait Time (hrs)'     ], \
        'ReqCPUS'     : ['Mean Requested CPU             ', '11.2f', 'M.ReqCPUS',            '.2f', 'Mean Requested CPU'             ], \
        'AllocCPUS'   : ['Mean Allocated CPU             ', '11.2f', 'M.AllocCPUS',          '.2f', 'Mean Allocated CPU'             ], \
        'AveDiskRead' : ['Mean Disk Read             (MB)', '11.2f', 'M.DiskRead (MB)',      '.2f', 'Mean Disk Read (MB)'            ], \
        'AveDiskWrite': ['Mean Disk Write            (MB)', '11.2f', 'M.DiskWrite (MB)',     '.2f', 'Mean Disk Write (MB)'           ], \
        'AveRSS'      : ['Mean RSS                   (MB)', '11.2f', 'M.RSS (MB)',           '.2f', 'Mean RSS (MB)'                  ], \
        'AveVMSize'   : ['Mean VM Size               (MB)', '11.2f', 'M.VMSize (MB)',        '.2f', 'Mean VM Size (MB)'              ], \
        'NSteps'      : ['Mean Number of Steps           ', '8d',    'M.NSteps',             'd',   'Mean Number of Steps'           ], \
        'JobID'       : ['JobID                          ', 's',     'JobID',                's',   'JobID'                          ], \
        'JobIndex'    : ['JobIndex                       ', 's',     'JobIndex',             's',   'JobIndex'                       ]  \
        }


    def __init__(self, jobs, ServUnits=0., CPUTime=0., filter={}):
        """
        Instantiates an instance from a job array.
        """
        self.data = {}
        self.CollectStats(jobs, filter)
        self.NormStats(ServUnits, CPUTime)


    def __getitem__(self, group):
        """
        Returns self.data[group].
        """
        if group in JobStats.GroupBase + JobStats.GroupExt:
            return self.data[group]
        else:
            raise KeyError('\"%s\" is an invalid JobStats group.' % group)


    def __setitem__(self, group, value):
        """
        Sets self.data[group].
        """
        if group in JobStats.GroupBase + JobStats.GroupExt:
            self.data[group] = value
        else:
            raise KeyError('\"%s\" is an invalid JobStats group.' % group)


    def CollectStats(self, jobs, filter={}):
        """
        Collects job stats from a job array.
        """
        if type(jobs) is not list:
            raise TypeError('\"%s\" is not a List.' % jobs)

        if type(filter) is not dict:
            raise TypeError('\"%s\" is not a Dict.' % filter)

        for group in filter.keys():
            if group not in JobStats.GroupBase + JobStats.GroupExt:
                raise KeyError('\"%s\" is an invalid JobStats group.' % group)

        for value in filter.values():
            if type(value) is not list:
                raise ValueError('\"%s\" is not a list.' % value)

        # Initializes data structure.
        for group in JobStats.GroupBase + JobStats.GroupExt:
            self[group] = {}

        # Collects stats from all jobs.
        for index, job in enumerate(jobs):

            # Verifies if this job matches the conditions defined in filter.
            match = True

            for group in filter.keys():
                if job[group] not in filter[group]:
                    match = False

            if not match:
                continue

            # JobStats.GroupBase
            for group in JobStats.GroupBase:
                key = job[group]

                if key not in self[group]:
                    self[group][key] = {}

                    for res in JobStats.Resources:
                        if res == 'NumJobs':
                            self[group][key][res] = 1

                        elif res in ['JobID']:
                            self[group][key][res] = [job[res]]

                        elif res in ['JobIndex']:
                            self[group][key][res] = [index]

                        elif res in ['SUPer', 'NTPer', 'CTPer', 'EfficiencyT', \
                            'EfficiencyU']:
                            pass

                        else:
                            self[group][key][res] = job[res]

                else:
                    for res in JobStats.Resources:

                        if res == 'NumJobs':
                            self[group][key][res] += 1

                        elif res in ['JobID']:
                            self[group][key][res] += [job[res]]

                        elif res in ['JobIndex']:
                            self[group][key][res] += [index]

                        elif res in ['SUPer', 'NTPer', 'CTPer', 'EfficiencyT', \
                            'EfficiencyU']:
                            pass

                        # timedelta fields
                        elif res in ['CPUTime', 'Elapsed', 'NodeTime', \
                            'Reserved', 'SystemCPU', 'Timelimit', 'TotalCPU', \
                            'UserCPU']:

                            try:
                                self[group][key][res] += job[res]
                            except OverflowError:
                                self[group][key][res] = timedelta.max

                        else:
                            self[group][key][res] += job[res]

            # JobStats.GroupExt
            for group1 in JobStats.GroupExt1:
                for group2 in JobStats.GroupExt2:

                    # Converts from strftime() output.
                    if group2 == 'Year':
                        key = eval('job[\'%s\'].strftime(\'%%Y\')' % group1)

                    elif group2 == 'Month':
                        key = eval('job[\'%s\'].strftime(\'%%m\')' % group1)

                    elif group2 == 'Day':
                        key = eval('job[\'%s\'].strftime(\'%%d\')' % group1)

                    elif group2 == 'Hour':
                        key = eval('job[\'%s\'].strftime(\'%%H\')' % group1)

                    elif group2 == 'Date':
                        key = eval('job[\'%s\'].strftime(\'%%Y-%%m-%%d\')' % \
                            group1)

                    elif group2 == 'Weekday':
                        key = eval('job[\'%s\'].strftime(\'%%w-%%a\')' % group1)

                    elif group2 == 'Week':
                        key = eval('job[\'%s\'].strftime(\'%%U\')' % group1)

                    group = group1 + group2

                    if key not in self[group]:
                        self[group][key] = {}

                        for res in JobStats.Resources:
                            if res == 'NumJobs':
                                self[group][key][res] = 1

                            elif res in ['JobID']:
                                self[group][key][res] = [job[res]]

                            elif res in ['JobIndex']:
                                self[group][key][res] = [index]

                            elif res in ['SUPer', 'NTPer', 'CTPer', \
                                'EfficiencyT', 'EfficiencyU']:
                                pass

                            else:
                                self[group][key][res] = job[res]

                    else:
                        for res in JobStats.Resources:
                            if res == 'NumJobs':
                                self[group][key][res] += 1

                            elif res in ['JobID']:
                                self[group][key][res] += [job[res]]

                            elif res in ['JobIndex']:
                                self[group][key][res] += [index]

                            elif res in ['SUPer', 'NTPer', 'CTPer', \
                                'EfficiencyT', 'EfficiencyU']:
                                pass

                            # timedelta fields
                            elif res in ['CPUTime', 'Elapsed', 'NodeTime', \
                                'Reserved', 'SystemCPU', 'Timelimit', 'TotalCPU', \
                                'UserCPU']:

                                try:
                                    self[group][key][res] += job[res]
                                except OverflowError:
                                    self[group][key][res] = timedelta.max

                            else:
                                self[group][key][res] += job[res]


    def NormStats(self, ServUnits=0., CPUTime=0.):
        """
        Normalizes job stats.
        """
        for group in JobStats.GroupBase + JobStats.GroupExt:

            # Normalizes efficiencies and time percentage usage.
            for key in self[group].keys():

                res = 'SUPer'
                try:
                    self[group][key][res] = \
                        total_seconds(self[group][key]['ServUnits']) / \
                        ServUnits * 100.

                except ZeroDivisionError:
                    timesum = timedelta(0)
                    for k in self[group].keys():
                        timesum += self[group][k]['ServUnits']

                    try:
                        self[group][key][res] = \
                            total_seconds(self[group][key]['ServUnits']) / \
                            total_seconds(timesum) * 100.

                    except ZeroDivisionError:
                        self[group][key][res] = 0.

                res = 'NTPer'
                try:
                    self[group][key][res] = \
                        total_seconds(self[group][key]['NodeTime']) / \
                        CPUTime * 100.

                except ZeroDivisionError:
                    timesum = timedelta(0)
                    for k in self[group].keys():
                        timesum += self[group][k]['NodeTime']

                    try:
                        self[group][key][res] = \
                            total_seconds(self[group][key]['NodeTime']) / \
                            total_seconds(timesum) * 100.

                    except ZeroDivisionError:
                        self[group][key][res] = 0.

                res = 'CTPer'
                try:
                    self[group][key][res] = \
                        total_seconds(self[group][key]['CPUTime']) / \
                        CPUTime * 100.

                except ZeroDivisionError:
                    timesum = timedelta(0)
                    for k in self[group].keys():
                        timesum += self[group][k]['CPUTime']

                    try:
                        self[group][key][res] = \
                            total_seconds(self[group][key]['CPUTime']) / \
                            total_seconds(timesum) * 100.

                    except ZeroDivisionError:
                        self[group][key][res] = 0.

                res = 'EfficiencyT'
                try:
                    self[group][key][res] = \
                        total_seconds(self[group][key]['TotalCPU']) / \
                        total_seconds(self[group][key]['NodeTime']) * 100.
                except ZeroDivisionError:
                    self[group][key][res] = 0.

                res = 'EfficiencyU'
                try:
                    self[group][key][res] = \
                        total_seconds(self[group][key]['UserCPU']) / \
                        total_seconds(self[group][key]['TotalCPU']) * 100.
                except ZeroDivisionError:
                    self[group][key][res] = 0.

            # Normalizes everything else.
            for res in JobStats.Resources:

                # fields that need to be skipped
                for key in self[group]:
                    if res in ['NumJobs', 'Charge', 'SUPer', 'NTPer', \
                        'CTPer', 'EfficiencyT', 'EfficiencyU']:
                        pass

                    elif res in ['JobID']:
                        self[group][key][res] = sorted(self[group][key][res])

                    elif res in ['JobIndex']:
                        pass

                    elif res in ['ServUnits', 'NodeTime', 'CPUTime']:
                        if self[group][key][res] != timedelta.max:
                            self[group][key][res] = total_seconds( \
                                self[group][key][res] ) / HOUR
                        else:
                            self[group][key][res] = 999999.99

                    elif res in ['TotalCPU', 'SystemCPU', 'UserCPU', \
                         'Timelimit', 'Elapsed', 'Reserved']:
                        if self[group][key][res] != timedelta.max:
                            self[group][key][res] = total_seconds( \
                                self[group][key][res] ) / HOUR / \
                                float(self[group][key]['NumJobs'])
                        else:
                            self[group][key][res] = 999999.99

                    elif res in ['AveDiskRead', 'AveDiskWrite', 'AveRSS', \
                        'AveVMSize', 'MaxRSS', 'MaxVMSize']:
                        self[group][key][res] /= MB * \
                            float(self[group][key]['NumJobs'])

                    else:
                        self[group][key][res] /= \
                            float(self[group][key]['NumJobs'])


    def CollectEvents(self, events, jobs):
        """
        Collect events for the replay engine.
        """
        info('Start collecting job events.')

        # Adds start(0) and end(1) events.
        for index, job in enumerate(jobs):

            # Only collect events with a reasonable start and end times.
            # 'Index' field stores the index of the job in the job list.
            # TODO: Need to consider DST, e.g., start: 1:50am, end: 1:20am
            if job['Start'] < job['End']:
                events.append({'Timestamp': job['Start'], \
                    'Event': EVENT_START, 'Index': index})
                events.append({'Timestamp': job['End'], \
                    'Event': EVENT_END, 'Index': index})

            else:
                debug('Ignore Job \"%s\" with an endtime not later than startime.' \
                    % job['JobID'])

        # Sorts events by timestamps.
        events.sort(key=lambda event: (event['Timestamp'], event['Event']))

        info('End collecting job events.')

        return events


    def NormEvents(self, events, jobs):
        """
        Normalizes job events to prepare for the usage chart.
        """
        info('Start normalizing job events.')

        for i in range(len(events)):
            index = events[i]['Index']
            job = jobs[index]

            # Job starts.
            if events[i]['Event'] == EVENT_START:
                if i != 0:
                    events[i]['Indices'] = list(events[i-1]['Indices'])
                    events[i]['Indices'].append(index)
                    events[i]['NJobs'] = events[i-1]['NJobs'] + 1
                    events[i]['NCPUs'] = events[i-1]['NCPUs'] + job['AllocCPUS']
                    events[i]['ECPUs'] = events[i-1]['ECPUs'] + \
                        job['AllocCPUS'] * job['EfficiencyT']
                    events[i]['Power'] = events[i-1]['Power'] + \
                        job['ConsumedEnergy']
                else:
                    events[i]['Indices'] = [index]
                    events[i]['NJobs'] = 1
                    events[i]['NCPUs'] = job['AllocCPUS']
                    events[i]['ECPUs'] = job['AllocCPUS'] * job['EfficiencyT']
                    events[i]['Power'] = job['ConsumedEnergy']

            # Job ends.
            elif events[i]['Event'] == EVENT_END:
                # Since we only count completed jobs, the first event is always
                # a 'start' event.
                events[i]['Indices'] = list(events[i-1]['Indices'])
                events[i]['Indices'].remove(index)
                events[i]['NJobs'] = events[i-1]['NJobs'] - 1
                events[i]['NCPUs'] = events[i-1]['NCPUs'] - job['AllocCPUS']
                events[i]['ECPUs'] = events[i-1]['ECPUs'] - \
                    job['AllocCPUS'] * job['EfficiencyT']
                events[i]['Power'] = events[i-1]['Power'] - \
                    job['ConsumedEnergy']

            else:
                warning('Unknown events.')

            # Counts NNodes.
            nodelist = []
            for index in events[i]['Indices']:
                nodelist.extend(jobs[index]['NodeList'])
            events[i]['NNodes'] = len(set(nodelist))

        info('End normalizing job events.')

        return events


    def OrderKeys(self, group, orderby='', reverse=False):
        """
        Reorders keys that is sorted with the "orderby" field.
        """
        if not orderby:
            return sorted(self[group].keys(), reverse=reverse)

        if orderby not in JobStats.Resources:
            raise RuntimeError('\"%s\" is an invalid Resource field.' % \
                orderby)

        tmp = {}

        for key in self[group].keys():
            tmp[key] = self[group][key][orderby]

        tmp = sort_dict_by_value(tmp, reverse)

        return [x[0] for x in tmp]


    def TextSec(self, group, key, indent='', delimiter=': ', \
            comprehensive=False):
        """
        Converts self[group][key] to text format.
        """
        if group not in JobStats.GroupBase + JobStats.GroupExt:
            raise KeyError('\"%s\" is an invalid JobStats group.' % group)

        if key not in self[group].keys():
            raise KeyError('\"%s\" is an invalid JobStats key.' % key)

        buf = ''

        resources = list(JobStats.Resources)

        if not comprehensive:
            resources.remove('JobID')
            resources.remove('JobIndex')

        for res in resources:
            val = self[group][key][res]
            fmt = '%%s%%s%%s%%%s\n' % JobStats.ResDef[res][1]
            buf += fmt % (indent, JobStats.ResDef[res][0], delimiter, val)

        return buf


    def ToText(self, group, indent='', delimiter=': ', orderby='', \
        reverse=False, title=False, comprehensive=False):
        """
        Converts self[group] to text format.
        """
        buf = ''

        for key in self.OrderKeys(group, orderby, reverse):
            if title:
                buf += indent + '%s - %s:\n' % (group, key)
            buf += self.TextSec(group, key, ' '*2+indent, delimiter, \
                comprehensive)
            buf += '\n'

        return buf


    def SummaryText(self, start, end, cputime, servunits, groups=[], \
        indent='', delimiter=': ', orderby='', reverse=False, \
        comprehensive=False):
        """
        Outputs job stats summary for this cluster.
        """
        info('Start generating TEXT Summary.')

        if not groups:
            groups = JobStats.GroupSum

        title = True
        buf = ''
        buf += 'Summary for period: (%s, %s)\n' % (start ,end)
        buf += '%d CPU Hours (%.2f Service Units) Delivered\n' % (cputime / \
            HOUR, servunits / HOUR)
        buf += '\n'

        for group in groups:
            buf += self.ToText(group, indent, delimiter, orderby, reverse, \
                title, comprehensive)
            buf += '\n'

        info('End generating TEXT Summary.')

        return buf


    def TableWidth(self, group):
        """
        Gets the field widths for a specific group.
        """
        if group not in JobStats.GroupBase + JobStats.GroupExt:
            raise KeyError('\"%s\" is an invalid JobStats group.' % group)

        width = {}

        # first column
        tmp = [len(key) for key in self[group].keys()]
        tmp.append(len(group))
        width[group] = max(tmp)

        resources = list(JobStats.Resources)
        resources.remove('JobID')
        resources.remove('JobIndex')

        for res in resources:
            tmp = []

            for key in self[group].keys():
                val = self[group][key][res]
                fmt = '%%%s' % (JobStats.ResDef[res][3])
                tmp.append(len(fmt % val))

            tmp.append(len(JobStats.ResDef[res][2]))
            width[res] = max(tmp)

        return width


    def TableHeader(self, group, indent='', delimiter='|', width={}, \
        leading=True, trailing=True):
        """
        Returns the header line for the row output.
        """
        if group not in JobStats.GroupBase + JobStats.GroupExt:
            raise KeyError('\"%s\" is an invalid JobStats group.' % group)

        if width:
            fmt = '%%%ds%%s' % (width[group])
        else:
            fmt = '%s%s'

        buf = fmt % (group, delimiter)

        resources = list(JobStats.Resources)
        resources.remove('JobID')
        resources.remove('JobIndex')

        for res in resources:
            if width:
                fmt = '%%%ds%%s' % (width[res])
            else:
                fmt = '%s%s'

            buf += fmt % (JobStats.ResDef[res][2], delimiter)

        if leading:
            buf = delimiter + buf

        if not trailing:
            buf = buf.strip(delimiter)

        buf = indent + buf + '\n'

        return buf


    def TableDivider(self, group, indent='', symbol='-', delimiter='+', \
        width={}, leading=True, trailing=True):
        """
        Returns a divider row.
        """
        if group not in JobStats.GroupBase + JobStats.GroupExt:
            raise KeyError('\"%s\" is an invalid group.' % group)

        if width:
            buf = '%s%s' % (width[group] * symbol, delimiter)
        else:
            buf = '%s%s' % (len(group) * symbol, delimiter)

        resources = list(JobStats.Resources)
        resources.remove('JobID')
        resources.remove('JobIndex')

        for res in resources:
            if width:
                buf += '%s%s' % (width[res] * symbol, delimiter)
            else:
                buf += '%s%s' %(len(res) * symbol, delimiter)

        if leading:
            buf = delimiter + buf

        if not trailing:
            buf = buf.strip(delimiter)

        buf = indent + buf + '\n'

        return buf


    def TableRow(self, group, key, indent='', delimiter='|', width={}, \
        leading=True, trailing=True):
        """
        Converts self[group][key] to a table row.
        """
        if group not in JobStats.GroupBase + JobStats.GroupExt:
            raise KeyError('\"%s\" is an invalid JobStats group.' % group)

        if key not in self[group].keys():
            raise KeyError('\"%s\" is an invalid JobStats key.' % key)

        if width:
            fmt = '%%%ds%%s' % (width[group])
        else:
            fmt = '%s%s'

        buf = fmt % (key, delimiter)

        resources = list(JobStats.Resources)
        resources.remove('JobID')
        resources.remove('JobIndex')

        for res in resources:
            val = self[group][key][res]

            if width:
                fmt = '%%%d%s%%s' % (width[res], JobStats.ResDef[res][3])
            else:
                fmt = '%%%s%%s' % (JobStats.ResDef[res][3])

            buf += fmt % (val, delimiter)

        if leading:
            buf = delimiter + buf

        if not trailing:
            buf = buf.strip(delimiter)

        buf = indent + buf + '\n'

        return buf


    def ToTable(self, group, indent='', symbol='-', delimiter='|', \
        orderby='', reverse=False, title=False, leading=True, trailing=True):
        """
        Converts self[group] to a table.
        """
        width = self.TableWidth(group)

        buf = ''

        if title:
            buf += indent + '%s:\n' % group

        buf += self.TableDivider(group, indent, symbol, '+', width, \
            leading, trailing)
        buf += self.TableHeader(group, indent, delimiter, width, leading, \
            trailing)
        buf += self.TableDivider(group, indent, symbol, '+', width, \
            leading, trailing)

        for key in self.OrderKeys(group, orderby, reverse):
            buf += self.TableRow(group, key, indent, delimiter, width, \
                leading, trailing)

        buf += self.TableDivider(group, indent, symbol, '+', width, \
            leading, trailing)

        return buf


    def SummaryTextTable(self, start, end, cputime, servunits, groups=[], \
        indent='', symbol='-', delimiter='|', orderby='', reverse=False, \
        leading=True, trailing=True):
        """
        Outputs summary in table format for this instance.
        """
        info('Start generating TEXT table Summary.')

        if not groups:
            groups = JobStats.GroupSum

        title = True
        buf = ''
        buf += 'Summary for period: (%s, %s)\n' % (start, end)
        buf += '%d CPU Hours (%.2f Service Units) Delivered\n' % (cputime / \
            HOUR, servunits / HOUR)
        buf += '\n'

        for group in groups:
            buf += self.ToTable(group, indent, symbol, delimiter, orderby, \
                reverse, title, leading, trailing)
            buf += '\n'

        info('End generating TEXT table Summary.')

        return buf


    def HTMLSec(self, group, key, indent='', comprehensive=False):
        """
        Converts self[group][key] to HTML format.
        """
        if group not in JobStats.GroupBase + JobStats.GroupExt:
            raise KeyError('\"%s\" is an invalid JobStats group.' % group)

        if key not in self[group].keys():
            raise KeyError('\"%s\" is an invalid JobStats key.' % key)

        buf = html_print_table_header(sorttable=False, width='50%')

        resources = list(JobStats.Resources)

        if not comprehensive:
            resources.remove('JobID')
            resources.remove('JobIndex')

        for res in resources:
            val = self[group][key][res]
            fmt = '%%%s' % JobStats.ResDef[res][3]
            buf += html_print_table_row( \
                [' '.join(JobStats.ResDef[res][0].split()), fmt % val])

        buf += html_print_table_footer()

        return buf


    def ToHTML(self, group, indent='', orderby='', reverse=False, title=False, \
        comprehensive=False):
        """
        Converts self[group] to HTML format.
        """
        buf = ''
        for key in self.OrderKeys(group, orderby, reverse):
            if title:
                buf += html_print_h2( \
                    html_print_anchor('%s - %s:' % (group, key), \
                    '%s_%s' % (group, key)))
            buf += self.HTMLSec(group, key, indent, comprehensive)

        return buf


    def SummaryHtml(self, start, end, cputime, servunits, groups=[], \
        indent='', orderby='', reverse=False, comprehensive=False):
        """
        Outputs summary in HTML format for this instance.
        """
        info('Start generating HTML Summary.')

        if not groups:
            groups = JobStats.GroupSum

        title = True
        buf = ''
        buf += html_print_header('Cluster Utilization Report')
        buf += '\n'
        buf += html_print_h1('Summary for period: (%s, %s)' % (start, end))
        buf += html_print_h2('%d CPU Hours (%.2f Service Units) Delivered' % \
            (cputime / HOUR, servunits / HOUR))
        buf += '\n'

        for group in groups:
            buf += self.ToHTML(group, indent, orderby, reverse, title, \
                comprehensive)
            buf += '\n'

        buf += html_print_divider()
        buf += html_print_footer()

        info('End generating HTML Summary.')

        return buf


    def HTMLTableHeader(self, group, indent=''):
        """
        Returns the header line for the HTML table.
        """
        if group not in JobStats.GroupBase + JobStats.GroupExt:
            raise KeyError('\"%s\" is an invalid JobStats group.' % group)

        resources = list(JobStats.Resources)
        resources.remove('JobID')
        resources.remove('JobIndex')

        row = [group] + [JobStats.ResDef[res][2] for res in resources]
        tips = [None] + [JobStats.ResDef[res][4] for res in resources]
        buf = html_print_table_header(row=row, tooltips=tips)

        return buf


    def HTMLTableFooter(self):
        """
        Returns the footer line for the HTML table.
        """
        return html_print_table_footer()


    def HTMLTableRow(self, group, key, indent=''):
        """
        Converts self[group][key] to a HTML table row.
        """
        if group not in JobStats.GroupBase + JobStats.GroupExt:
            raise KeyError('\"%s\" is an invalid JobStats group.' % group)

        if key not in self[group].keys():
            raise KeyError('\"%s\" is an invalid JobStats key.' % key)

        resources = list(JobStats.Resources)
        resources.remove('JobID')
        resources.remove('JobIndex')

        buf = ''

        tmp = [key]

        for res in resources:
            val = self[group][key][res]
            fmt = '%%%s' % JobStats.ResDef[res][3]
            tmp.append(fmt % val)

        buf += html_print_table_row(tmp, align='right', highlight=True)

        return buf


    def ToHTMLTable(self, group, indent='', orderby='', reverse=False, \
        title=False):
        """
        Converts self[group] to a HTML table.
        """
        buf = ''

        if title:
            buf += html_print_h2(html_print_anchor(group, group))

        buf += self.HTMLTableHeader(group, indent)

        for key in self.OrderKeys(group, orderby, reverse):
            buf += self.HTMLTableRow(group, key, indent)

        buf += self.HTMLTableFooter()

        return buf


    def SummaryHtmlTable(self, start, end, cputime, servunits, nnodes, ncpus, \
        powerbase, powerpeak, jobs, groups=[], indent='', orderby='', \
        reverse=False, image=True):
        """
        Outputs summary in HTML table for this instance.
        """
        info('Start generating HTML Table Summary.')

        if not groups:
            groups = JobStats.GroupSum

        title = True

        # Date format to be used in file names.
        Start = start.strftime('%Y-%m-%d')
        End   = end.strftime('%Y-%m-%d')

        buf = ''
        buf += html_print_header('Cluster Utilization Report')
        buf += '\n'
        buf += html_print_h1('Summary for period: (%s, %s)' % (start, end))
        buf += html_print_h2('%d CPU Hours (%.2f Service Units) Delivered' % \
            (cputime / HOUR, servunits / HOUR))
        buf += '\n'

        # Return if no stats available.
        if (not self['Cluster']):
            buf += 'No statistics available.\n'
            buf += html_print_divider()
            buf += html_print_footer()
            return buf

        for group in groups:

            # table
            buf += self.ToHTMLTable(group, indent, orderby, reverse, title)

            if image:
                # Generates charts.
                info('Start generating \"%s\" plot.' % group)

                self.BarChart(group, 'NumJobs', \
                    '%s_%s_%s_%s_bar.png' % (group, 'NumJobs', Start, End))
                self.BarChart(group, 'ServUnits', \
                    '%s_%s_%s_%s_bar.png' % (group, 'ServUnits', Start, End))
                self.PieChart(group, 'ServUnits', \
                    '%s_%s_%s_%s_pie.png' % (group, 'ServUnits', Start, End))
                self.BarChart(group, 'Reserved', \
                    '%s_%s_%s_%s_bar.png' % (group, 'Reserved', Start, End))

                info('End generating \"%s\" plot.' % group)

                # images
                buf += html_print_table_header(border='0')
                buf += html_print_table_row([ \
                    html_print_img( \
                    '%s_%s_%s_%s_bar.png' % (group, 'NumJobs', Start, End), \
                    '%s_%s_%s_%s_bar' % (group, 'NumJobs', Start, End)), \
                    html_print_img( \
                    '%s_%s_%s_%s_bar.png' % (group, 'ServUnits', Start, End), \
                    '%s_%s_%s_%s_bar' % (group, 'ServUnits', Start, End)), \
                    html_print_img( \
                    '%s_%s_%s_%s_pie.png' % (group, 'ServUnits', Start, End), \
                    '%s_%s_%s_%s_pie' % (group, 'ServUnits', Start, End)), \
                    html_print_img( \
                    '%s_%s_%s_%s_bar.png' % (group, 'Reserved', Start, End), \
                    '%s_%s_%s_%s_bar' % (group, 'Reserved', Start, End)), \
                    ], align='center')
                buf += html_print_table_footer()

            # Cluster usage and JobSize distribution plots.
            if group == 'Cluster' and image:
                # JobSize distribution plot.
                self.JobSizeHistChart('AllocCPUS_%s_%s_hist.png' % (Start, End))

                buf += html_print_table_header(border='0')
                buf += html_print_table_row([ \
                    html_print_img('AllocCPUS_%s_%s_hist.png' % (Start, End), \
                    'AllocCPUS_%s_%s_hist.png' % (Start, End))], align='center')
                buf += html_print_table_footer()

                # Usage plot.
                info('Start generating usage plot.')

                events = []
                events = self.CollectEvents(events, jobs)
                events = self.NormEvents(events, jobs)

                self.UsageChart(events, start, end, nnodes, ncpus, powerbase, \
                    powerpeak, '%s_%s_%s_line.png' % (group, Start, End))

                info('End generating usage plot.')

                buf += html_print_table_header(border='0')
                buf += html_print_table_row([ \
                    html_print_img('%s_%s_%s_line.png' % (group, Start, End), \
                    '%s_%s_%s_line.png' % (group, Start, End))], align='center')
                buf += html_print_table_footer()

            buf += '\n'

        buf += html_print_divider()
        buf += html_print_footer()

        info('End generating HTML Table Summary.')

        return buf


    def UsageChart(self, events, start, end, nnodes, ncpus, powerbase, \
        powerpeak, filename=''):
        """
        Plots cluster usage line chart.
        """
        from matplotlib import pyplot as plt
        from matplotlib import dates as dates

        if not filename:
            filename = 'Cluster.png'

        ts = [] # timestamps
        nn = [] # NNodes
        nc = [] # NCPUs
        ec = [] # ECPUs
        po = [] # Power

        # Duplicate event points.
        for i in range(len(events)):
            ts.append(events[i]['Timestamp'])
            ts.append(events[i]['Timestamp'])

            if i != 0:
                nn.append(nn[2*i-1])
                nc.append(nc[2*i-1])
                ec.append(ec[2*i-1])
                po.append(po[2*i-1])
            else:
                nn.append(0.)
                nc.append(0.)
                ec.append(0.)
                po.append(0.)

            nn.append(float(events[i]['NNodes']) / float(nnodes) * 100.)
            nc.append(float(events[i]['NCPUs']) / float(ncpus) * 100.)
            ec.append(float(events[i]['ECPUs']) / float(ncpus) * 100.)
            po.append((events[i]['Power'] + powerbase) / 1000.)

        # Formatting.
        if (end - start) <= timedelta(days=3):
            ds = dates.drange(start, end, timedelta(hours=1))
            fmt = dates.DateFormatter('%Y-%m-%d %H:%M')

        elif (end - start) <= timedelta(days=30):
            ds = dates.drange(start, end, timedelta(days=1))
            fmt = dates.DateFormatter('%Y-%m-%d')

        elif (end - start) <= timedelta(days=90):
            ds = dates.drange(start, end, timedelta(weeks=1))
            fmt = dates.DateFormatter('%Y-%m-%d')
        else:
            ds = dates.drange(start, end, timedelta(weeks=4))
            fmt = dates.DateFormatter('%Y-%m')

        # Plotting.
        fig = plt.figure(1, figsize=(10,5))
        ax1 = fig.add_subplot(1,1,1)
        ln1 = ax1.plot(ts, nn, 'r-', label='Node')
        ln2 = ax1.plot(ts, nc, 'g-', label='CPU')
        ln3 = ax1.plot(ts, ec, 'b-', label='Effective CPU')
        ax1.set_xlim(ds[0], ds[-1])
        ax1.set_ylim(0, 100)
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Usage (%)')
        ax1.grid()
        ax1.xaxis.set_major_formatter(fmt)
        fig.autofmt_xdate()
        ax2 = ax1.twinx()
        ln4 = ax2.plot(ts, po, 'y-', label='Power')
        ax2.set_xlim(ds[0], ds[-1])
        ax2.set_ylim(powerbase/1000., powerpeak/1000.)
        ax2.set_ylabel('Power (kW)')
        ax2.xaxis.set_major_formatter(fmt)
        fig.autofmt_xdate()
        plt.setp(ax2.get_xticklabels(), visible=False)
        lns = ln1 + ln2 + ln3 + ln4
        labs = [l.get_label() for l in lns]
        ax2.legend(lns, labs, loc='best')
        plt.savefig(filename, bbox_inches='tight')
        plt.close()


    def JobSizeHistChart(self, filename=''):
        """
        Plots the job size distribution chart.
        """
        from matplotlib import pyplot as plt

        if not filename:
            filename = 'AllocCPUS.png'

        ncpus = [int(self['JobID'][j]['AllocCPUS']) for j in self['JobID']]
        base = [1,2,4,8,16,32,64,128,256,512,1024]
        fig = plt.figure(1, figsize=(5,5))
        ax = fig.add_subplot(1,1,1)
        ax.hist(ncpus, facecolor='green', bins=base)
        ax.set_xscale('log', basex=2)
        ax.set_xticklabels(base)
        ax.set_xlabel('AllocCPUS')
        ax.set_ylabel('NumJobs')
        ax.set_title('Cluster - AllocCPUS', bbox={'facecolor':'0.8', 'pad':5})
        plt.savefig(filename, bbox_inches='tight')
        plt.close()


    def BarChart(self, group, res, filename=''):
        """
        Plots self[group][key][res] on a bar chart.
        """
        from matplotlib import pyplot as plt

        if not filename:
            filename = '%s_%s.png' % (group, res)

        labels = sorted(self[group].keys())
        values = [self[group][key][res] for key in labels]

        width = 0.5
        fig = plt.figure(1, figsize=(5,5))
        ax = fig.add_subplot(1,1,1)
        ax.bar([x+width/2 for x in range(len(values))], values, width, \
            color='r')
        ax.set_xticks([x+width for x in range(len(values))])
        ax.set_xticklabels(labels)
        ax.set_ylabel(JobStats.ResDef[res][2])
        ax.yaxis.grid(True)
        ax.set_title('%s - %s' % (group, res), bbox={'facecolor':'0.8', \
            'pad':5})
        fig.autofmt_xdate()
        plt.savefig(filename, bbox_inches='tight')
        plt.close()


    def PieChart(self, group, res, filename=''):
        """
        Plots self[group][key][res] on a pie chart.
        """
        from matplotlib import pyplot as plt

        if not filename:
            filename = '%s_%s.png' % (group, res)

        labels = sorted(self[group].keys())
        values = [self[group][key][res] for key in labels]
        total = sum(values)
        try:
            fracs = [float(x) / float(total) for x in values]
        except ZeroDivisionError:
            fracs = [0.0 for x in values]

        fig = plt.figure(1, figsize=(5,5))
        ax = fig.add_subplot(1,1,1)
        ax.pie(fracs, labels=labels, autopct='%.2f%%', shadow=True)
        ax.set_title('%s - %s' % (group, res), bbox={'facecolor':'0.8', \
            'pad':5})
        plt.savefig(filename, bbox_inches='tight')
        plt.close()



# functions
def total_seconds(td):
    """
    Converts a datetime.timedelta object to total seconds. This implements 
    timedelta.total_seconds() which is available in Python 2.7.
    """
    try:
        t = (td.microseconds + (td.seconds + td.days * DAY) * 10.**6) / 10.**6
    except:
        t = 0.
    return t


def cmp_float(var1, var2):
    """
    Compares two floats with potential string inputs. Returns True if var1 >
    var2, False if var <= var2.
    """
    try:
        var1 = float(var1)
    except:
        var1 = 0.

    try:
        var2 = float(var2)
    except:
        var2 = 0.

    if var1 > var2:
        t = True
    else:
        t = False

    return t


def exp_range(rng):
    """
    Expand a range string '01-02' to a sequence ['01', '02'].
    """
    result = set()
    strlen = []
    maxlen = 0

    for part in rng.split(','):
        x = part.split('-')

        # Handles reverse order as well.
        if int(x[0]) < int(x[-1]):
            start = int(x[0])
            end = int(x[-1])
        else:
            start = int(x[-1])
            end = int(x[0])

        result.update(range(start, end+1))
        strlen.extend([len(y.strip()) for y in x])

    if len(set(strlen)) > 1:
        maxlen = 0
    else:
        maxlen = max(strlen)

    return ['%0*d' % (maxlen, i) for i in result]


def exp_rangestr(var):
    """
    Expand a regular expression with prefix and suffix to a sequence. E.g., 
    n000[1-2].lr1 -> ['n0001.lr1', 'n0002.lr2'].
    """
    prefix, repeat, suffix = var.groups()
    repeat = exp_range(repeat)
    result = ['%s%s%s' % (prefix, x, suffix) for x in repeat]

    return ','.join(result)


def exp_noderangestr(var):
    """
    Expand a node range string to a sequence. E.g.,
    n000[1-2].lr1 -> ['n0001.lr1', 'n0002.lr2'].
    """
    return re.sub(r'([\w\-]*)\[([\d\s\-\,]+)\]([\.\w]*)', exp_rangestr, \
        var).split(',')


def exp_cpulist(job, nodes_conf, shared):
    """
    Expand a job's NodeList to CPUList that it was executed on.
    """
    cpulist = []

    # For dedicated nodes.
    if not shared:
        for node in job['NodeList']:
            try:
                cpulist += [nodes_conf[node]['Name']] * nodes_conf[node]['PPN']
            except KeyError:
                debug('Job \"%s\" has an undefined node \"%s\", ignore.' % \
                    (job['JobID'], node))

    # For shared nodes.
    elif job['NNodes']:
        # For equalily distributed layout.
        # TODO: Assume all nodes are evenly distributed.
        if job['AllocCPUS'] % job['NNodes'] == 0:
            ppn = job['AllocCPUS'] / job['NNodes']
            for node in job['NodeList']:
                try:
                    cpulist += [nodes_conf[node]['Name']] * ppn
                except KeyError:
                    debug('Job \"%s\" has an undefined node \"%s\", ignore.' % \
                        (job['JobID'], node))

        # For non-equally distributed layout.
        # TODO: Assume we are always using the 'cyclic' layout. Needs
        #       improvement for others, especially arbitrary layout.
        else:
            cpus = 0
            while cpus < job['AllocCPUS']:
                for node in job['NodeList']:
                    try:
                        cpus += 1
                        cpulist += [nodes_conf[node]['Name']]
                    except KeyError:
                        debug('Job \"%s\" has an undefined node \"%s\", ignore.' \
                            % (job['JobID'], node))

    return cpulist


def norm_timestamp(var):
    """
    Normalize a timestamp string in the format of 'YYYY-MM-DD[THH:MM[:SS]]' to a
    datetime value.
    """
    TS_FMT = '^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})(T(?P<hour>\d{2}):(?P<minute>\d{2})(:(?P<second>\d{2}))?)?$'
    TS_RE  = re.compile(TS_FMT)

    match = TS_RE.match(var)

    if match:
        year   = match.group('year')
        month  = match.group('month')
        day    = match.group('day')
        hour   = match.group('hour')
        minute = match.group('minute')
        second = match.group('second')

        if second is None:
            second = 0
        if minute is None:
            minute = 0
        if hour is None:
            hour = 0

        return datetime(year=int(year), month=int(month), day=int(day), \
            hour=int(hour), minute=int(minute), second=int(second))

    elif var == 'Unknown':
        warning('Invalid timestamp string \"%s\", reset to now.' % var)
        return datetime.now()

    else:
        raise ValueError('Invalid timestamp string \"%s\".' % var)


def norm_timelapse(var):
    """
    Normalize a time elapse string in the format of '[D#-][HH:][MM:]SS[.MMM]'
    to a timedelta value.
    """
    TE_FMT = '^((((?P<days>\d+)-)?(?P<hours>\d{2}):)?(?P<minutes>\d{2}):)?(?P<seconds>\d{2})(\.(?P<milsecs>\d{3}))?$'
    TE_RE  = re.compile(TE_FMT)

    match = TE_RE.match(var)

    if match:
        days    = match.group('days')
        hours   = match.group('hours')
        minutes = match.group('minutes')
        seconds = match.group('seconds')
        milsecs = match.group('milsecs')

        if milsecs is None:
            milsecs = 0
        if minutes is None:
            minutes = 0
        if hours is None:
            hours = 0
        if days is None:
            days = 0

        return timedelta(days=int(days), hours=int(hours), \
            minutes=int(minutes), seconds=int(seconds), \
            microseconds=int(milsecs)*1000)

    elif var == '':
        debug('Null timelapse string, reset to 0.')
        return timedelta(0) 

    elif var == 'UNLIMITED':
        return timedelta.max

    elif var == 'INVALID':
        debug('Invalid timelapse string \"%s\", reset to 0.' % var)
        return timedelta(0)

    elif var == 'Partition_Limit':
        debug('Invalid timelapse string \"%s\", reset to 0.' % var)
        return timedelta(0)

    else:
        raise ValueError('Invalid timelapse string \"%s\".' % var)


def sizeof_fmt(num, suffix='B'):
    """
    Convert storage number to human readable format.
    """
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < KB:
            return "%6.2f%s%s" % (num, unit, suffix)
        num /= KB
    return "%.2f%s%s" % (num, 'Yi', suffix)
