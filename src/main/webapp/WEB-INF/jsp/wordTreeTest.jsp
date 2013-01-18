<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <div id="test"></div>

    <script src="<c:url value="/static/js/jquery/jquery.js"/>" type="text/javascript"></script>
    <script src="<c:url value="/static/js/wordtree/wordtree.js"/>" type="text/javascript"></script>
    <script src="<c:url value="/static/js/wordtree/raphael.js"/>" type="text/javascript"></script>
    <script src="<c:url value="/static/js/wordtree/word-tree-layout.js"/>" type="text/javascript"></script>

    <script type="text/javascript">
        (function() {

            function createTree(data) {
                var context = "yesterday";

                var lefts = [
                    ["I", "was", "coming", "home"],
                    ["He", "was", "coming", "home", "from", "work"],
                    ["I", "was", "coming", "out", "of", "the", "shops"],
                    ["While", "I", "was", "going", "home"],
                    ["She", "was", "coming", "home"]
                ];
                for(var i = 0; i < lefts.length; i++){
                    lefts[i] = lefts[i].reverse();
                }
                var rights = [
                    ["when", 'a', 'rabbit', 'ate', 'a', 'mouse', "."],
                    ["but", 'a', 'rabbit', 'ate', 'a', 'rat', "."],
                    ["then", 'a', 'rat', 'ate', 'a', 'mouse', "."],
                    [",", 'the', 'rat', 'ate', 'a', 'mouse', "."],
                    ["and", 'the', 'cat', 'ate', 'a', 'mouse', "."],
                    ["when", 'the', 'cat', 'ate', 'a', 'mouse', "."],
                    ["when",'the', 'cat', 'ate', 'the', 'mouse', "."]
                ];
                var w = 1000,
                        h = 150,
                        detail = 100 /* % */;
                var paper = Raphael("test", w, h);
                makeWordTree(rights, context, detail, "test", w, h, WordTree.RO_LEFT, paper);
                makeWordTree(lefts, context, detail, "test", w, h, WordTree.RO_RIGHT, paper);
            }

            YAHOO.util.Event.onContentReady("test", function() {
                createTree("");
            });
        })();

    </script>
</layout:main>