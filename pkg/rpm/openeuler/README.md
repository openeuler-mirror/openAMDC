### Generate RPM files for the openAMDC
Dependencies:
```bash
yum install -y rpm-build
```

Usage: 
```bash
$ cd openAMDC/pkg/rpm/openeuler
$ sudo ./build_rpm.sh
$ cd openAMDC/pkg/rpm/openeuler/rpm_files_generated
$ rpm -i openAMDC-*.rpm
$ systemctl start openamdc.service
```

Uninstall
```bash
$ rpm -e openamdc
```
