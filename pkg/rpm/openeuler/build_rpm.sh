#! /bin/bash

### usage sudo ./build_rpm
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT=$DIR/../../../
BUILD=$DIR/../../../src
version=$(grep -w OPENAMDC_VERSION $BUILD/version.h | awk '{ printf $3 }' | tr -d \")
release=1 # by default this will always be 1 for openAMDC version structure. If build release version needs to be update you can modify here
arch=$(uname -m)
dist=oe$(cat /etc/openEuler-release | tr -dc '0-9')

if [[ "$arch" != "aarch64" ]] && [[ "$arch" != "x86_64" ]]; then
	echo "This script is only valid and tested for aarch64 and x86_64 architectures. You are trying to use: $arch"
fi

# remove any old rpm packages
rm -rf $DIR/rpm_files_generated/openamdc*

# build openamdc
make -C $ROOT -j$(nproc)

# generate empty directories that gitee would otherwise delete (avoids .gitkeep in directory)
mkdir -p $DIR/openamdc_build/openamdc_rpm/usr/bin
mkdir -p $DIR/openamdc_build/openamdc_rpm/usr/lib64
mkdir -p $DIR/openamdc_build/openamdc_rpm/var/lib/openamdc
mkdir -p $DIR/openamdc_build/openamdc_rpm/var/log/openamdc
mkdir -p $DIR/openamdc_build/openamdc_rpm/etc/openamdc

# move binaries to bin
cp $BUILD/openamdc-server $DIR/openamdc_build/openamdc_rpm/usr/bin/
cp $BUILD/openamdc-cli $DIR/openamdc_build/openamdc_rpm/usr/bin/
cp $BUILD/openamdc-benchmark $DIR/openamdc_build/openamdc_rpm/usr/bin/
cp $BUILD/../openamdc.conf $DIR/openamdc_build/openamdc_rpm/etc/openamdc/
cp $BUILD/../sentinel.conf $DIR/openamdc_build/openamdc_rpm/etc/openamdc/

# update spec file with build info
sed -i '2d' $DIR/openamdc_build/openamdc.spec
sed -i -E "1a\Version     : $version" $DIR/openamdc_build/openamdc.spec
sed -i '3d' $DIR/openamdc_build/openamdc.spec
sed -i -E "2a\Release     : $release%{?dist}" $DIR/openamdc_build/openamdc.spec

mkdir -p /root/rpmbuild/BUILDROOT/openamdc-$version-$release.$arch
cp -r $DIR/openamdc_build/openamdc_rpm/* /root/rpmbuild/BUILDROOT/openamdc-$version-$release.$arch/
rpmbuild -bb $DIR/openamdc_build/openamdc.spec
mv /root/rpmbuild/RPMS/$arch/* $DIR/rpm_files_generated
rm -rf $DIR/openamdc_build/openamdc_rpm/usr/bin/*
rm -rf $DIR/openamdc_build/openamdc_rpm/etc/openamdc/*
rm -rf $DIR/installed
rm -rf $DIR/is
rm -rf $DIR/not
mv $DIR/rpm_files_generated/openamdc-$version-$release.$arch.rpm $DIR/rpm_files_generated/openamdc-$version-$release.$dist.$arch.rpm

exit
