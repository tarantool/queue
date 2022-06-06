Name: tarantool-queue
Version: 1.0.1
Release: 1%{?dist}
Summary: Persistent in-memory queues for Tarantool
Group: Applications/Databases
License: BSD
URL: https://github.com/tarantool/queue
Source0: https://github.com/tarantool/%{name}/archive/%{version}/%{name}-%{version}.tar.gz
BuildArch: noarch
BuildRequires: tarantool >= 1.7
BuildRequires: tarantool-devel >= 1.7
BuildRequires: /usr/bin/prove
Requires: tarantool >= 1.7
%description
A collection of persistent queue implementations for Tarantool.

%prep
%setup -q -n %{name}-%{version}

%build
%cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo

# %check
# make check
#
%define luapkgdir %{_datadir}/tarantool/queue/
%install
%if 0%{?fedora} >= 33 || 0%{?rhel} >= 8
  %cmake_install
%else
  %make_install
%endif

%files
%dir %{luapkgdir}
%{luapkgdir}/*.lua
%{luapkgdir}/abstract/*.lua
%{luapkgdir}/abstract/driver/*.lua
%doc README.md
%{!?_licensedir:%global license %doc}
%license LICENSE

%changelog
* Wed Apr 18 2018 Alexander Turenko <alexander.turenko> 1.0.1
- Update tarantool dependency to 1.7

* Thu Apr 06 2016 Eugene Blikh <bigbes@tarantool.org> 1.0.1-6
- RPM spec uses CMake now (depend on tarantool0devel)

* Thu Feb 18 2016 Roman Tsisyk <roman@tarantool.org> 1.0.0-1
- Initial version of the RPM spec
