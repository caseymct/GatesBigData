var SEARCHAUTOCOMPLETE = {};
SEARCHAUTOCOMPLETE.ui = {};
SEARCHAUTOCOMPLETE.util = {};

(function() {
    var Connect         = YAHOO.util.Connect,              ButtonGroup     = YAHOO.widget.ButtonGroup,
        XHRDataSource = YAHOO.util.XHRDataSource,      AutoComplete    = YAHOO.widget.AutoComplete,
        Event           = YAHOO.util.Event,                Json            = YAHOO.lang.JSON,
        Dom             = YAHOO.util.Dom;

    var suggestUrl = "";

    SEARCHAUTOCOMPLETE.ui.setSuggestUrl = function(url) {
        suggestUrl = url;
    };

    var ds = new XHRDataSource(suggestUrl);
    ds.responseType = XHRDataSource.TYPE_JSON;
    ds.responseSchema = { resultsList : "suggestions" };

    var itemSelectHandler = function(s, args) {
        var inputEl = Dom.get(args[0].getInputEl());
        var sel = args[2][0].match("<b>(.*)</b> <i>(.*)</i>.*");
        inputEl.value = sel[2] + ":\"" + sel[1] + "\"";
    };

    var ac = new AutoComplete("autocomplete_input", "autocomplete_container", ds);
    ac.generateRequest = function() {
        return '?n=5&core=' + SEARCH.ui.coreName + '&userinput=' + this.getInputEl().value.encodeForRequest();
    };
    ac.itemSelectEvent.subscribe(itemSelectHandler);

    Event.addListener("autocomplete_input", "keyup", function(e) {
        if (this.value == "") {
            FACET.ui.buildInitialFacetTree();
        }
    });

})();