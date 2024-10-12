import cdawmeta

logger = None

cldefs = cdawmeta.cli('report.py', defs=True)
report_names = cldefs['report-name']['choices']

clargs = cdawmeta.cli('report.py')
report_name = clargs['report_name']
del clargs['report_name']

if report_name is not None:
  report_names = [report_name]

for report_name in report_names:
  report_func = getattr(cdawmeta.reports, report_name)
  datasets = report_func(clargs)
