Name        : openamdc
Version     : 0.1.0
Release     : 1%{?dist}
Group       : Unspecified
License     : MulanPSL v2
Packager    : Apusic Inc.
Summary     : Open Advanced In-Memory Data Cache

# scripts

#preinstall scriptlet (using /bin/sh):
%pre
getent group openamdc &> /dev/null || \
groupadd -r openamdc &> /dev/null
getent passwd openamdc &> /dev/null || \
useradd -r -g openamdc -d /var/lib/openamdc -s /sbin/nologin \
-c 'open Advanced In-Memory Data Cache' openamdc &> /dev/null
exit 0


#postinstall scriptlet (using /bin/sh):
%post
if [ $1 -eq 1 ] ; then 
        # Initial installation 
        systemctl preset openamdc.service >/dev/null 2>&1 || : 
fi 

if [ $1 -eq 1 ] ; then 
        # Initial installation 
        systemctl preset openamdc-sentinel.service >/dev/null 2>&1 || : 
fi

chown -R openamdc:openamdc /etc/openamdc
chown -R openamdc:openamdc /var/log/openamdc
chown -R openamdc:openamdc /var/lib/openamdc
chown -R openamdc:openamdc /usr/libexec/openamdc-shutdown


#preuninstall scriptlet (using /bin/sh):
%preun
if [ $1 -eq 0 ] ; then 
        # Package removal, not upgrade 
        systemctl --no-reload disable openamdc.service > /dev/null 2>&1 || : 
        systemctl stop openamdc.service > /dev/null 2>&1 || : 
fi 


if [ $1 -eq 0 ] ; then 
        # Package removal, not upgrade 
        systemctl --no-reload disable openamdc-sentinel.service > /dev/null 2>&1 || : 
        systemctl stop openamdc-sentinel.service > /dev/null 2>&1 || : 
fi


#postuninstall scriptlet (using /bin/sh):
%postun
systemctl daemon-reload >/dev/null 2>&1 || : 
if [ $1 -ge 1 ] ; then 
        # Package upgrade, not uninstall 
        systemctl try-restart openamdc.service >/dev/null 2>&1 || : 
fi 


systemctl daemon-reload >/dev/null 2>&1 || : 
if [ $1 -ge 1 ] ; then 
        # Package upgrade, not uninstall 
        systemctl try-restart openamdc-sentinel.service >/dev/null 2>&1 || : 
fi


%Description
OpenAMDC is an key-value store. It is often referred to as a data structure server 
since keys can contain strings, hashes, lists, sets and sorted sets.


%files
/etc/logrotate.d/openamdc
/etc/systemd/system/openamdc.service.d/limit.conf
/etc/systemd/system/openamdc-sentinel.service.d/limit.conf
/usr/bin/*
/usr/lib/systemd/system/*
/usr/libexec/*
/var/lib/*
/var/log/*
/etc/openamdc/*