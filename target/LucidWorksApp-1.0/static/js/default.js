var LWA = {};
LWA.ui = {};

(function() {

    LWA.ui.removeDivChildNodes = function(divName) {
        var div = Dom.get(divName);

        while (div.hasChildNodes()) {
            while (div.childNodes.length >= 1 ) {
                div.removeChild(div.firstChild);
            }
        }
    };

    LWA.ui.createDomElement = function (elType, parentNode, attributes) {
        var el = document.createElement(elType);
        parentNode.appendChild(el);

        for(var i = 0; i < attributes.length; i++) {
            if (attributes[i].key.match(/text/)) {
                el.innerHTML = attributes[i].value;
            } else {
                el.setAttribute(attributes[i].key, attributes[i].value);
            }
        }
        return el;
    };
})();
