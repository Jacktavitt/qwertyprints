import time


def make_time_string(times_labeled):
    '''
        expects list of tuples (name, timestart, timedone)
    '''
    ts = ''
    for tup in times_labeled:
        temp = "{}: {} seconds\n".format(tup[0], abs(tup[1]-tup[2]))
        ts = ts + temp
    return ts

def write_time_log(times_string, logname, conf=None):
    fn = '{}_timelog.txt'.format(logname)
    with open(fn, 'a+') as lf:
        lf.write("================================================================")
        lf.write(times_string)
        if conf:
            lf.write(str(conf))
    
