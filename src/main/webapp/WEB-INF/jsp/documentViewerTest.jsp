<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <div style="font-weight:bold; margin-bottom: 5px">File:
        <span style="font-weight:normal" id="viewerheader"></span>
    </div>

    <div id="documentviewer"></div>
    <script src="<c:url value="/static/js/prizm/swfobject.2.2.js"/>" type="text/javascript"></script>


    <script type="text/javascript">
        (function() {
            var     Dom = YAHOO.util.Dom,     Event = YAHOO.util.Event,
                Connect = YAHOO.util.Connect,  Json = YAHOO.lang.JSON;

            var remoteSeg  = YAHOO.deconcept.util.getRequestParameter("segment");
            var remoteFile = YAHOO.deconcept.util.getRequestParameter("file");
            var coreName   = YAHOO.deconcept.util.getRequestParameter("core");
            var urlParams  = "?segment=" + remoteSeg + "&core=" + coreName + "&file=" + remoteFile;

            Dom.get("viewerheader").innerHTML = remoteFile;

            var swfParams = {
                wmode: 'opaque',
                scale: 'noscale',
                allowFullScreen: true,
                allowScriptAccess: 'always',
                bgcolor: '#ffffff'
            };
            var swfAttributes = {
                id: 'Viewer',
                name: 'Viewer'
            };

            Connect.asyncRequest('GET', '<c:url value="/document/writelocal" />' + urlParams, {
                success : function(o) {
                    var result = o.responseText;
                    if (result.match(/<h1>Error<\/h1>/)) {
                        Dom.get("documentviewer").innerHTML = result;
                    } else {

                        var flashvars = {
                            documentname: result.replace(/\\/g, '/'),
                            conversionLink: '<c:url value="/document/convert"/>'
                        };

                        swfobject.embedSWF('<c:url value="/static/prizm/Viewer.swf"/>' + '?time=' + new Date(), 'documentviewer',
                                '620', '800', '11.1.102', false, flashvars, swfParams, swfAttributes);
                    }

                },
                failure : function(o) {
                    alert("Could not write local file");
                }
            });


        })();
    </script>
</layout:main>
