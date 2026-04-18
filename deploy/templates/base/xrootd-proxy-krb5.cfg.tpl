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
    xrootd.seclib libXrdSeckrb5.so
    xrootd.seclib libXrdSec.so
    sec.protocol unix
    sec.protocol krb5 /etc/xrootd/krb5.keytab ${xrootd_krb5_principal}
    sec.protocol sss -s /etc/xrootd/sss.keytab -c /etc/xrootd/sss.keytab
    sec.protbind * only krb5 unix sss
    acc.audit deny grant
fi

all.adminpath /var/spool/xrootd
all.pidpath /run/xrootd

continue /etc/xrootd/config.d/
