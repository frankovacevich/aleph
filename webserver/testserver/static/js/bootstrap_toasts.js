class MyToast{
    constructor(toast_id, parent_div_id, title, small, text){
        this.id = toast_id;
        this.parent_div_id = parent_div_id;
        this.title = title;
        this.small = small;
        this.text = text;

        this.delay = 5000;

        this.create = function(){
            let html = '';
            html += '<div id="toast_' + this.id + '" class="toast" role="alert" aria-live="assertive" aria-atomic="true">';
            html += '<div class="toast-header">';
            html += '<strong class="mr-auto">' + title + '</strong>';
            html += '<small class="text-muted">' + small + '</small>';
            html += '<button type="button" class="ml-2 mb-1 close" data-dismiss="toast" aria-label="Close">';
            html += '<span aria-hidden="true">&times;</span>';
            html += '</button>';
            html += '</div>';
            html += '<div class="toast-body">';
            html += text;
            html += '</div>';
            html += '</div>';

            document.getElementById(parent_div_id).innerHTML += html;
            $('.toast').toast({delay: this.delay});
        }

        this.show = function(){
            var parent = this;
            window.setTimeout(function(){ $("#toast_" + parent.id).toast("show"); }, 500);
            window.setTimeout(function(){ $("#toast_" + parent.id).toast("hide"); }, parent.delay);
        }
    }
}

var __toast_id__ = 0;

function show_toast(parent_div_id, title, small, text){
    __toast_id__ += 1;
    var new_toast = new MyToast(__toast_id__, parent_div_id, title, small, text)
    new_toast.create();
    new_toast.show();
}