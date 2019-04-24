(function($){
    $(document).ready(function(){
        //$('.tabular.menu .item').tab()
        //$.tab('change tab','tab-account');
        //$.tab('change tab','tab-intro');
        //console.log("bootstrap");
        $("#startnotebook").on('click', function() {
            thebelab.on("status", function(evt, data) {
                console.log("Status changed:", data.status, data.message);
                $(".thebe-status-field")
                  .attr("class", "thebe-status-field thebe-status-" + data.status)
                  .text(data.status);
            });
            thebelab.bootstrap();
        });
    });
})(this.jQuery)
