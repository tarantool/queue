Name: tarantool-queue
Version: 1.0.0
Release: 1%{?dist}
Summary: Persistent in-memory queues for Tarantool
Group: Applications/Databases
License: BSD
URL: https://github.com/tarantool/queue
Source0: https://github.com/tarantool/%{name}/archive/%{version}/%{name}-%{version}.tar.gz
BuildArch: noarch
BuildRequires: tarantool >= 1.6.8.0
BuildRequires: /usr/bin/prove
Requires: tarantool >= 1.6.8.0
%description
A collection of persistent queue implementations for Tarantool.

%prep
%setup -q -n %{name}-%{version}

%check
make test

%define luapkgdir %{_datadir}/tarantool/queue/
%install
install -d -m 0755 %{buildroot}%{luapkgdir}
install -m 0644 queue/*.lua %{buildroot}%{luapkgdir}/
install -d -m 0755 %{buildroot}%{luapkgdir}/abstract/
install -m 0644 queue/abstract/*.lua %{buildroot}%{luapkgdir}/abstract/
install -d -m 0755 %{buildroot}%{luapkgdir}/abstract/driver/
install -m 0644 queue/abstract/driver/*.lua %{buildroot}%{luapkgdir}/abstract/driver/

%files
%dir %{luapkgdir}
%{luapkgdir}/*.lua
%{luapkgdir}/abstract/*.lua
%{luapkgdir}/abstract/driver/*.lua
%doc README.md
%{!?_licensedir:%global license %doc}
%license LICENSE

%changelog
* Thu Feb 18 2016 Roman Tsisyk <roman@tarantool.org> 1.0.0-1
- Initial version of the RPM spec
