<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <script type="text/javascript">
    (function() {
       // var url = window.location.href.split('/');
      //  var coreName = url[url.length - 1];
        var coreName = YAHOO.deconcept.util.getRequestParameter("core");
        debugger;
        window.open('<c:url value="/search/" />' + coreName, "_self");
    })();

    </script>
</layout:main>