(function($){
    $(document).ready(function(){
        $("#submitdiv").hide();
        //$('.tabular.menu .item').tab()
        //$.tab('change tab','tab-account');
        //$.tab('change tab','tab-intro');
        //console.log("bootstrap");
        $("#submitanswers").on('click', function() {
            var submission = {
                questions: {},
                code: [],
            }
            $(".questionfield").toArray().forEach(function(e) {
                var label = $(e).find("label")[0].innerText;
                var answer = $(e).find("textarea")[0].value;
                console.log(label,":", answer);
                submission.questions[label] = answer;
            });
            var i = 0;
            $(".CodeMirror-code").toArray().forEach(function(e) {
                i++;
                if (i == 1) { return }
                submission.code.push(e.innerText);
            });


            $.post("/postform", JSON.stringify(submission), function(e) {
                $("#submitdiv").show();
                $("#submitdiv").addClass("positive");
                $("#submitmsg").text("successful!");
            })
              .fail(function(e) {
                    $("#submitdiv").show();
                    $("#submitdiv").addClass("negative");
                    console.error(e)
                    $("#submigmsg").text("unsuccessful!");
                    });
             //var myWindow = window.open("", "MsgWindow", "width=200,height=100");

             //myWindow.document.write(JSON.stringify(submission)); 


            console.log(submission);
        });

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
