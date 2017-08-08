from spt3g import core

def write_toast_cache_to_spt3g(data, filename, timestreams='total',
                               verbose=False):
    '''
    Dump the local TOAST cache to disk in SPT3G format. Will extract data of
    timestreams given by the timestreams argument (e.g. cache entries
    named "total_detA" for the default timestreams value ("total") and a
    detector "detA". If verbose is True, will also print frames to the console.

    Note that this does *not* include chunks from any remote nodes. For this to
    be useful, you *must* swizzle the data around ahead of time such that nodes
    are chunked by time with each node holding the full set of detectors you
    wish to include in the files. This will hopefully be improved later.
    '''
    writer = core.G3Writer(filename) # XXX: Multiwriter?

    for obs in data.obs:
        f = core.G3Frame(core.G3FrameType.Observation)
        obsname = obs['name'].split('-')
        f['SourceName'] = '-'.join(obsname[:-1])
        f['ObservationID'] = int(obsname[-1])
        if 'id' in obs:
            f['ToastID'] = obs['id']

        if verbose:
            print(f)
        writer(f)
        
        # Loop through intervals
        starts = [i.first for i in obs['intervals']]
        stops = [i.last+1 for i in obs['intervals']] # 'last' is inclusive
        intervals = sorted([(*i, True) for i in zip(starts, stops)] +
                           [(*i, False) for i in zip(stops[:-1], starts[1:])])
        tod = obs['tod']

        times = tod.read_times()*core.G3Units.s # UNIX time

        for i in intervals:
            f = core.G3Frame(core.G3FrameType.Scan)
            obsname = obs['name'].split('-')
            f['SourceName'] = '-'.join(obsname[:-1])
            f['ObservationID'] = int(obsname[-1])

            if i[2]:
                f['Selected'] = True

            # Get start and stop time, skipping zero-length intervals and
            # clamping to available data
            start = i[0] if i[0] >= tod.local_samples[0] else \
                    tod.local_samples[0]
            stop = i[1] if i[1] >= tod.local_samples[0] else \
                    tod.local_samples[0]
            if start >= tod.local_samples[-1]:
                continue
            if stop >= tod.local_samples[-1]:
                stop = tod.local_samples[-1]

            startt = core.G3Time(times[start])
            stopt = core.G3Time(times[stop])
            if startt == stopt:
                continue

            # Boresight Az (El unavailable as of 8/7/17)
            az = core.G3Timestream(tod.read_boresight_az()[start:stop]* \
                 core.G3Units.rad)
            az.start = startt
            az.stop = stopt
            f['BoresightAz'] = az

            tsm = core.G3TimestreamMap()
            for d in tod.local_dets: # XXX remote?
                ts = core.G3Timestream(
                     tod.cache.reference(timestreams + '_' + d)[start:stop],
                     units=core.G3TimestreamUnits.Kcmb) # XXX: units?
                ts.start = startt
                ts.stop = stopt
                tsm[d] = ts
            f[timestreams] = tsm

            # XXX: any other metadata we want to put in?
            if verbose:
                print(f)
            writer(f)

    writer(core.G3Frame(core.G3FrameType.EndProcessing))

