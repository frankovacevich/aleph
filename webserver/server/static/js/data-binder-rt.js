function getCookie(name) { let cookieValue = null; if (document.cookie && document.cookie !== '') { const cookies = document.cookie.split(';'); for (let i = 0; i < cookies.length; i++) { const cookie = cookies[i].trim(); if (cookie.substring(0, name.length + 1) === (name + '=')) { cookieValue = decodeURIComponent(cookie.substring(name.length + 1)); break; } } } return cookieValue; }
const csrftoken = getCookie('csrftoken');

var mqtt_username = "receiver";
var mqtt_password = "CW43mHchekNDTzZtm2LugA622AHY3jFgSW6A2PRX5KsfSyh3HnuY7Gnq2d928L2n";


// Creates an mqtt client and subscribes to the given topics
// Example: mqtt_connect("http://127.0.0.1:8083","u","p",["topic"])
function mqtt_connect(server, username, password, subscription_topics = [], extra_options = {}){
    const options = { 
        clean: true, 
        connectTimeout: 4000, 
        clientId: '', 
        username: username, 
        password: password
    };
    for(const opt in extra_options) options[opt] = extra_options[opt];

    // create mqtt client
    const client = mqtt.connect(server, options);

    // on connect, subscribe to given topics
    client.on("connect", function(){
        console.log("mqtt client connected");
        for(const topic in subscription_topics){
            client.subscribe(subscription_topics[topic]);
        }
        return;
    });


    return client;

    // to publish use client.publish(topic, message)
}


class DataBinder{

	// ties an html object to a data source.
	// when new data arrives on the connection provided, the
	// function on_new_data is executed and updates the object

	constructor(since=1, until=0){
		this.topics = {};
		this.binded = [];

		this.since = since;
		this.until = until;

        let server = "";
        if(window.location.protocol == "https:"){
            server = "wss://" + window.location.hostname + "/mqtt";
        } else {
            server = "ws://" + window.location.hostname + ":1883";
        }

		this.mqtt_client = mqtt_connect(server, mqtt_username, mqtt_password)
		var parent = this;

		this.mqtt_client.on("message", function(topic, message){
			parent.process_message(topic, message, true);
		});

        this.mqtt_client.on("offline", function(){ document.getElementById("disconnected_overlay").style.display = "flex" })
        this.mqtt_client.on("connect", function(){ document.getElementById("disconnected_overlay").style.display = "none" })
	}

	bind_object(object_id, topic, field, count, on_new_data, after = function(){}){

		// add topic
        if(!(topic in this.topics)){
            topic = topic.replaceAll("/", ".")
            this.topics[topic] = {fields: [], count: 0, done: false};
            this.mqtt_client.subscribe(topic.replaceAll(".", "/"));
        }

        if(!this.topics[topic].fields.includes(field)){
            this.topics[topic].fields.push(field);
            if(count > this.topics[topic].count) this.topics[topic].count = count;
        }

        // add object
        this.binded.push({
            "topic": topic,
            "field": field,
            "object_id": object_id,
            "on_new_data": on_new_data,
            "after": after,
        });
	}

	refresh(){
        var parent = this;

		for(const topic in this.topics){
            let post_ = { 
                csrfmiddlewaretoken: csrftoken, 
                fields: this.topics[topic].fields, 
                since: this.since, 
                until: this.until, 
                count: this.topics[topic].count,
            };

            $.post("/api/namespace/" + topic + "/data", post_, function(result){
                if(result.length > 0){
                    result.reverse();
                    for(var r=0; r<result.length; r++){
                        console.log(topic, result[r]);
                        parent.process_message(topic, result[r], r+1 == result.length);
                    }
                }
            });
        }
	}

	process_message(topic, message, do_after = false){
        topic = topic.replaceAll("/", ".");
        let msg = message;

        // if message is a json string, turn into object
        if(msg.constructor != Object){
            msg = JSON.parse(message);
        }

        for(const item in this.binded){
            let obj = this.binded[item];

            if(obj.topic == topic && obj.field in msg){
                obj.on_new_data(obj.object_id, msg[obj.field], msg["t"]);
                if(do_after) obj.after();
            }
            else if(obj.topic == topic && obj.field == "*"){
                obj.on_new_data(obj.object_id, msg, msg["t"]);
                if(do_after) obj.after();
            }
        }
    }

    publish(key, data, on_success=function(){}){
        try{    
            let post_ = { 
                ccsrfmiddlewaretoken: csrftoken, 
                data: JSON.stringify(data) 
            }

            $.post("/api/namespace/" + key + "/publish", post_, function(result){
                if("r" in result){
                    if(result.r == 0){
                        show_toast("toasts_area","Success","","Data published");
                        on_success();
                    } else {
                        show_toast("toasts_area","<a style='color:red'>Error</a>","",result.result);
                    }
                }
            }).fail(function(result){
                let msg = "Hubo un error en la conexión con el servidor. Intente actualizando la página."
                if(result.status == 403){ msg = "Usuario sin permisos para escribir"; }
                show_toast("toasts_area","<a style='color:red'>Error</a>","",msg);
            });

        }catch(err){
            show_toast("toasts_area","<a style='color:red'>Error</a>","",err.message);
        }
    }
    

    publish_from_table(key, table_object, on_success=function(){}){
        let changes = table_object.get_changes();
        let data = [];
        for(const id_ in changes){
            changes[id_]["id_"] = id_;
            data.push(changes[id_]);
        }
        this.publish(key, data, on_success);
    }

}
