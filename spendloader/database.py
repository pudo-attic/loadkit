from spendloader.table import get_records


def database_load(engine, package):
    
    for record in get_records(package):
        print 'REC', record
