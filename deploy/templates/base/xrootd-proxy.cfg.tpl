set xrdport=1098
set cmsdport=1213

xrootd.trace all debug
ofs.trace all
xrd.trace all
cms.trace all
sec.trace all debug

if exec cmsd
    all.role server
    xrd.port $$cmsdport
    all.export / stage
else
    xrd.localroot /xrootd
    oss.localroot /xrootd
    all.export / stage
    all.role server
    xrd.port $$xrdport
    sec.protocol unix
    sec.protbind * only unix
    acc.audit deny grant
fi

all.adminpath /var/spool/xrootd
all.pidpath /run/xrootd
