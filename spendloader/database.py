from spendloader.table import Table


def database_load(engine, package):
    
    for record in Table(package).records():
        print 'REC', record
