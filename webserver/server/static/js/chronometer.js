class Chronometer{
	
	constructor(){
		this.instances = {};

		this.pad = function(x){
			if(x<10) return "0" + x;
			return x;
		}
	}

	set_instance(name, fun, t0 = undefined){
		if(t0 == undefined){
			this.instances[name] = {"t0": new Date(), "f": fun, "enabled": true};	
		} else {
			this.instances[name] = {"t0": new Date(t0), "f": fun, "enabled": true};
		}
		return;
	}

	start(instance_name){
		var parent = this;
		window.setInterval(function(){
			var instance = parent.instances[instance_name];
			if(instance.enabled){				
				var now = new Date();
				var seconds = Math.floor((now - instance.t0) / (1000));
				var minutes = Math.floor(seconds / 60);
				var hours = Math.floor(seconds / 3600);

				var min_sec = parent.pad(minutes % 60) + ":" + parent.pad(seconds % 60);

				var time = parent.pad(hours % 60) + ":" + min_sec;

				parent.instances[instance_name].f({seconds: seconds, minutes: minutes, hours: hours, time: time, min_sec: min_sec});
			}
		}, 1000);
	}

	stop(instance_name){
		this.instances[instance_name].enabled = false;
	}

}
