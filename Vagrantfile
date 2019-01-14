
Vagrant.configure("2") do |config|
    config.vm.synced_folder ".", "/vagrant", disabled: true
    config.vm.box = "ubuntu/trusty64"
    config.vm.provision "file", source: "main", destination: "main"
    
    config.vm.define "dsv1" do |dsv| 
        dsv.vm.network "private_network", ip: "192.168.0.11"
        dsv.vm.hostname = "dsv1"
    end
    config.vm.define "dsv2" do |dsv| 
        dsv.vm.network "private_network", ip: "192.168.0.12"
        dsv.vm.hostname = "dsv2"
    end
    config.vm.define "dsv3" do |dsv| 
        dsv.vm.network "private_network", ip: "192.168.0.13"
        dsv.vm.hostname = "dsv3"
    end
    config.vm.define "dsv4" do |dsv| 
        dsv.vm.network "private_network", ip: "192.168.0.14"
        dsv.vm.hostname = "dsv4"
    end
end
