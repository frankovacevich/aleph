function getCookie(name) { let cookieValue = null; if (document.cookie && document.cookie !== '') { const cookies = document.cookie.split(';'); for (let i = 0; i < cookies.length; i++) { const cookie = cookies[i].trim(); if (cookie.substring(0, name.length + 1) === (name + '=')) { cookieValue = decodeURIComponent(cookie.substring(name.length + 1)); break; } } } return cookieValue; }
const csrftoken = getCookie('csrftoken');


class DataBinder{

    // ties an html object to a data source.
    // when new data arrives on the connection provided, the
    // function on_new_data is executed and updates the object

    constructor(since=1, until=0, autorefresh=true){
        this.topics = []; // keys on the namespace to query
        this.binded = []; // 

        this.since = since;
        this.until = until;

        if(autorefresh){
            var parent = this;
            window.setInterval(function(){parent.refresh(1)}, 5000);    
        }
    }

    bind_object(object_id, topic, field, count, on_new_data, after = function(){}){
        // add topic
        if(!this.topics.includes(topic)){
            topic = topic.replaceAll("/", ".")
            this.topics.push(topic);
        }

        // add object
        this.binded.push({
            "topic": topic,
            "field": field,
            "object_id": object_id,
            "on_new_data": on_new_data,
            "after": after,
            "count": count,
            "last_t": 0,
        });

        // download last values
        this.refresh();
    }

    refresh(count_limit=0){
        var parent = this;
        for(const item in this.binded){
            var obj = this.binded[item];
            var topic = obj.topic;
            var field = obj.field;
            var count = count_limit > 0 ? count_limit : obj.count;

            $.post("/api/namespace/" + topic + "/data", 
                { csrfmiddlewaretoken: csrftoken, fields: [field], since: this.since, until: this.until, count: count }, 
                function(result){
                if(result.length > 0){
                    result.reverse();
                    for(const r in result){
                        parent.process_message(topic, result[r]);    
                    }
                }
                obj.after();
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
            if("id_" in msg){
                if(new Date(msg["t_"]) <= new Date(obj.last_t)) return;
            } else {
                if(new Date(msg["t"]) <= new Date(obj.last_t)) return;
            }
            
            obj.last_t = msg["t_"];

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

}