from bs4 import BeautifulSoup as bs
from shlex import split
import subprocess as sb
from time import ctime, sleep

comm_prefix = '/opt/google/bin/ge'

_taskpriority = {
    'resourcetask': 0,
    'projecttask': 1,
    'databasetask': 2,
}

def _parsestr(xml):
    dom = bs(xml, 'xml').fusiontasks
    tasks = [i for i in dom
        if i.name is not None and i.name.endswith('task')]
    
    assets = [i for i in tasks
        if i.name in ('resourcetask', 'projecttask', 'databasetask')]
    assets.sort(key=lambda a: _taskpriority[a.name])
    
    builds = [i for i in dom.children if i.name == 'buildtask']

    publishes = [i for i in dom.children if i.name == 'publishtask']

    return {
        'assets': assets,
        'builds': builds,
        'publishes': publishes
    }



def _genasset(taskdom):
    verifytype = lambda t: taskdom.name.startswith(t)
    if verifytype('resource'):
        gen = _geres
    elif verifytype('project'):
        gen = _genprj
    elif verifytype('database'):
        gen = _gendb
    else:
        raise NotImplementedError(taskdom.name)
    comm = gen(taskdom)
    def handle():
        print('COMMAND:', comm)
        sb.check_call(split(comm))
        print('done at', ctime())
    return handle


def _geres(taskdom):
    comm = comm_prefix
    comm += 'new{}resource -o {} '.format(
        taskdom['type'],
        taskdom.assetname.get_text(),
    )

    if taskdom.src:
        comm += taskdom.src.get_text()
    elif taskdom.listfile:
        comm += '--filelist ' + taskdom.listfile.get_text()
    else:
        raise FileNotFoundError()
    
    if taskdom.sourcedate:
        comm += ' --sourcedate ' + taskdom.sourcedate.get_text()
    else:
        comm += ' --sourcedate 1970-01-01 '

    return comm


def _genprj(taskdom):
    comm = comm_prefix
    comm += 'new{}project -o {} '.format(
        taskdom['type'],
        taskdom.assetname.get_text(),
    )
    comm += ' '.join([x.get_text()
        for x in taskdom.resources.find_all('resource')])
    return comm


def _gendb(taskdom):
    comm = comm_prefix
    comm += 'newdatabase -o {} '.format(
        taskdom.assetname.get_text(),
    )
    prj_added = False
    for x in taskdom.children:
        if x.name is None or not x.name.endswith('project'):
            continue
        prjtype = x.name[:-len('project')]
        if prjtype in ('imagery', 'vector', 'terrain'):
            prj_added = True
            comm += '-{} {} '.format(
                prjtype,
                x.get_text(),
            )
    
    if not prj_added:
        raise AttributeError()
    return comm

def _genbuild(taskdom):
    asset = taskdom.asset.get_text()
    def handle():
        comm = comm_prefix + 'build ' + asset
        print('COMMAND:', comm)
        sb.check_call(split(comm))
        interval = 1.5
        while True:
            sleep(interval)
            output = sb.check_output(split(
                comm_prefix + 'query --status ' + asset
            )).decode().strip()
            if output == 'Succeeded':
                break
            else:
                dependencies = sb.check_output(split(
                    comm_prefix + 'query --dependencies ' + asset
                )).decode().strip().splitlines()[1:-4]
                nodupdepens = (i for i in dependencies if not i.startswith('R'))
                statuses = [i.split()[-1] for i in nodupdepens]
                print(
                    statuses.count('Succeeded'), '/', len(statuses),
                    '    ',
                    ctime()
                )
        print('build finished at', ctime())
    return handle


# should it be separated from other tasks(since requiring sudo privilege?
def _genpublish(taskdom):
    db = taskdom.database.get_text()
    targetpath = taskdom.targetpath.get_text()

    temp = comm_prefix + 'serveradmin --{} ' + db + ' '

    add = temp.format('adddb')
    push = temp.format('pushdb')
    publish = temp.format('publishdb') + '--targetpath ' + targetpath
    
    def handle():
        print('push to geserver')
        for c in (add, push, publish):
            print('COMMAND:', c)
            sb.check_call(split(c))
            print('finished at ', ctime())
        print('database {} successfully published on geserver/{}'.format(db, targetpath))
    return handle


def generate_funcs(xml):
    tasks = _parsestr(xml)
    funcs = []
    for i in tasks['assets']:
        funcs.append(_genasset(i))
    for i in tasks['builds']:
        funcs.append(_genbuild(i))
    for i in tasks['publishes']:
        funcs.append(_genpublish(i))
    return funcs


def main():
    print('running test...')
    with open('fusionwarpper/template/singleimg_simple.xml') as f:
        xml = f.read()
    from sys import argv
    testdir = argv[1]
    targetpath = argv[2]
    xml = xml.format_map({
        'assetdir': testdir,
        'sourcefile': '/gevol/src/fusion/Imagery/usgsLanSat.tif',
        'targetpath': targetpath,
    })
    for f in generate_funcs(xml):
        f()


if __name__ == '__main__':
    main()
