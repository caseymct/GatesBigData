var QUERYBUILDER = {};
QUERYBUILDER.ui = {};
QUERYBUILDER.util = {};

(function() {
    var Connect         = YAHOO.util.Connect,              ButtonGroup     = YAHOO.widget.ButtonGroup,
        LocalDataSource = YAHOO.util.LocalDataSource,      AutoComplete    = YAHOO.widget.AutoComplete,
        Event           = YAHOO.util.Event,                Json            = YAHOO.lang.JSON,
        Dom             = YAHOO.util.Dom;

    var buttonTextToDelimiterMap = { 'Some' : 'OR', 'All' : 'AND' };

    var genQuerySearchInputElName       = "general_query_search_input",
        fieldConstraintElName           = "general_query_field_constraint",
        fieldAutoCompleteName           = "general_query_field_autocomplete",
        fieldAutoCompleteContainerName  = "general_query_field_autocomplete_container",
        hasPhraseFieldElName            = "has_phrase",
        isExactFieldElName              = "is_exact",
        hasWordsFieldElName             = "has_words",
        doesNotHaveWordsFieldElName     = "does_not_have_words",
        hasWordsButtonGroupDiv          = "has_words_div",
        doesNotHaveWordsButtonGroupDiv  = "does_not_have_words_div",
        addToQueryElName                = "add_to_query",
        generalQueryTabElName           = "search_generalquery_tab",
        fieldsetRowCSSClass             = "fieldset_row",
        fieldsetRowButtonDivCSSClass    = "fieldset_row_button_div",
        fieldNonEmptyCheckBox           = "field_non_empty",
        fieldMustExistCheckBox          = "field_must_exist",
        clearBothCSSClass               = "clearboth";

    var populateFieldAutoCompleteUrl = "";
    var constraintFieldNames = [fieldConstraintElName, hasPhraseFieldElName, isExactFieldElName, hasWordsFieldElName, doesNotHaveWordsFieldElName];

    QUERYBUILDER.ui.getGeneralQuerySearchInputElName = function() {
        return genQuerySearchInputElName;
    };

    function getButtonGroupValue(bg) {
        return buttonTextToDelimiterMap[UI.getButtonGroupCheckedButtonValue(bg)];
    }

    function clearConstraintFields() {
        for(var i = 0; i < constraintFieldNames.length; i++) {
            Dom.get(constraintFieldNames[i]).value = "";
        }
    }

    QUERYBUILDER.ui.init = function(names) {
        populateFieldAutoCompleteUrl = names.url;
        buildHTML(names.tabListElName, names.tabContentElName);
    };

    function adjustGeneralQueryByConstraint(hasWordsButtonGroup, doesNotHaveWordsButtonGroup) {
        var f, generalQueryInput = Dom.get(genQuerySearchInputElName);
        var currStr = generalQueryInput.value;
        if (currStr == "*:*") currStr = "";

        var field                = Dom.get(fieldConstraintElName).value,
            phrase               = Dom.get(hasPhraseFieldElName).value,
            exact                = Dom.get(isExactFieldElName).value,
            fieldNonEmptyChecked = Dom.get(fieldNonEmptyCheckBox).checked,
            fieldExistsChecked   = Dom.get(fieldMustExistCheckBox).checked;

        var wordsArr    = Dom.get(hasWordsFieldElName).value.split(/[\s,]+/),
            notwordsArr = Dom.get(doesNotHaveWordsFieldElName).value.split(/[\s,]+/),
            words       = wordsArr.join(' ' + getButtonGroupValue(hasWordsButtonGroup) + ' '),
            notwords    = notwordsArr.join(' ' + getButtonGroupValue(doesNotHaveWordsButtonGroup) + ' '),
            newStr      = "",
            match       = currStr.match(field + ':'),
            matchEnd    = currStr.match('(' + field + ':\\(.+\\))$'),
            matchMiddle = currStr.match('(' + field + ':\\(.+\\)) [AND|OR]+ \\w+[\\.\\w]*:');

        if (words == '' && notwords == '' && phrase == '' && exact == '' && fieldNonEmptyChecked == false && fieldExistsChecked == false) return;

        if (match == null && currStr.indexOf(':') > -1 && currStr.endsWith([' AND ', ' OR ']) == false) {
            currStr += ' AND ';
        }
        if (exact != "") {
            newStr = field + ':"' + exact + '"';
        } else {
            var p = [];
            if (words !== '') p.push((wordsArr.length == 1) ? words : '(' + words + ')');
            if (notwords !== '') p.push('NOT ' + ((notwordsArr.length == 1) ? notwords : '(' + notwords + ')'));
            if (phrase !== '') {
                if (!(phrase.startsWith('"') && phrase.endsWith('"'))) phrase = '"' + phrase + '"';
                p.push(phrase);
            }
            if (p.length > 0) {
                newStr = field + ':(' + p.join(' AND ') + ')';
            }
        }

        if (match != null) {
            generalQueryInput.value = currStr.replace((matchMiddle != null) ? matchMiddle[1] : matchEnd[1], newStr);
        } else {
            generalQueryInput.value = currStr + newStr;
        }

        if (fieldNonEmptyChecked) {
            f = ' -' + field + ':(\"\")';
            if (generalQueryInput.value.match(f) == null) generalQueryInput.value += f;
        }
        if (fieldExistsChecked) {
            f = ' ' + field + ':[* TO *]';
            if (generalQueryInput.value.match(f) == null) generalQueryInput.value += f;
        }

        clearConstraintFields();
    }

    Event.onContentReady(generalQueryTabElName, function() {
        /* Query constraint buttongroup code */
        var doesNotHaveWordsButtonGroup = new ButtonGroup(doesNotHaveWordsButtonGroupDiv);
        doesNotHaveWordsButtonGroup.check(1);
        var hasWordsButtonGroup = new ButtonGroup(hasWordsButtonGroupDiv);
        hasWordsButtonGroup.check(1);

        /* Populate the autocomplete field */
        Connect.asyncRequest('GET', populateFieldAutoCompleteUrl, {
            success: function(o) {
                var response = Json.parse(o.responseText);
                var fieldac = new AutoComplete(fieldConstraintElName, fieldAutoCompleteContainerName, new LocalDataSource(response));
                fieldac.typeAhead = true;
            }
        });

        Event.addListener(addToQueryElName, "click", function(e) {
            Event.stopEvent(e);
            adjustGeneralQueryByConstraint(hasWordsButtonGroup, doesNotHaveWordsButtonGroup);
        });
    });

    function buildHTML(tabListEl, tabContentEl) {
        var li = UI.addDomElementChild('li', Dom.get(tabListEl));
        UI.addDomElementChild('a', li, { href : "#", innerHTML: "<em>Construct General Query</em>"});

        var tabNode = Dom.get(tabContentEl);
        var p = UI.addDomElementChild('div', tabNode, {id : generalQueryTabElName }, {class : "search_tab_style row"});

        var txtAreaDiv = UI.addDomElementChild('div', p, null, {class : "row", padding : "2px"});
        UI.addDomElementChild('textarea', txtAreaDiv, {id : genQuerySearchInputElName, value : "*:*"}, null);

        var fieldset = UI.addDomElementChild('fieldset', p, null, { "padding-top": 0} );
    
        var legend = UI.addDomElementChild('legend', fieldset, null, { class : "search_legend"});
        UI.addDomElementChild('a', legend, {id : addToQueryElName, href : "#", innerHTML : "Add a constraint"}, null);

        var d = UI.addDomElementChild('div', fieldset, null, { class : fieldsetRowCSSClass });
        UI.addDomElementChild('label', d, {for : fieldConstraintElName, innerHTML: "Field name:" }, {"margin-top" : "10px"});
        var d1 = UI.addDomElementChild('div', d, { id : fieldAutoCompleteName }, null);
        UI.addDomElementChild('input', d1, { id : fieldConstraintElName },  null);
        UI.addDomElementChild('div', d1, { id : fieldAutoCompleteContainerName }, null);

        // Has words
        d = UI.addDomElementChild('div', fieldset, null, { class : fieldsetRowCSSClass });
        UI.addDomElementChild('label', d, { id : hasWordsFieldElName + "_label", for : hasWordsFieldElName, innerHTML : "Has words:"}, null);
        
        d1 = UI.addDomElementChild('div', d, { id : hasWordsButtonGroupDiv }, { class: fieldsetRowButtonDivCSSClass });
        UI.addDomElementChild('input', d1, { id : hasWordsFieldElName + "_some", type : "radio", name : hasWordsFieldElName + "_input", value : "Some"});
        UI.addDomElementChild('input', d1, { id : hasWordsFieldElName + "_all",  type : "radio", name : hasWordsFieldElName + "_input", value : "All"});
        UI.addDomElementChild('input', d, { id : hasWordsFieldElName });

        // does not have words
        d = UI.addDomElementChild('div', fieldset, null, { class : fieldsetRowCSSClass });
        UI.addDomElementChild('label', d, { id : doesNotHaveWordsFieldElName + "_label", for : doesNotHaveWordsFieldElName, 
            innerHTML : "Does not have words:" }, null);
        d1 = UI.addDomElementChild('div', d, { id : doesNotHaveWordsButtonGroupDiv}, { class : fieldsetRowButtonDivCSSClass });
        UI.addDomElementChild('input', d1, { id : doesNotHaveWordsFieldElName + "_some", type : "radio",
                name : doesNotHaveWordsFieldElName + "_input", value : "Some"});
        UI.addDomElementChild('input', d1, { id : doesNotHaveWordsFieldElName + "_all", type : "radio",
                name : doesNotHaveWordsFieldElName + "_input", value : "All"});
        UI.addDomElementChild('input', d, { id : doesNotHaveWordsFieldElName }, null);

        // has phrase
        d = UI.addDomElementChild('div', fieldset, null, { class : fieldsetRowCSSClass, "padding-top":"10px" });
        UI.addDomElementChild('label', d, { id : hasPhraseFieldElName + "_label", for : hasPhraseFieldElName, innerHTML : "Has phrase:" }, null);
        UI.addDomElementChild('input', d, { id : hasPhraseFieldElName }, null);

        // is exactly
        d = UI.addDomElementChild('div', fieldset, null, { class : fieldsetRowCSSClass, "padding-top":"10px" });
        UI.addDomElementChild('label', d, { id : isExactFieldElName + "_label", for:isExactFieldElName, innerHTML:"Is exactly:" }, null);
        UI.addDomElementChild('input', d, { id : isExactFieldElName }, null);

        // checkboxes
        d = UI.addDomElementChild('div', fieldset, null, { class : fieldsetRowCSSClass,  "padding-top": "10px" });
        UI.addDomElementChild('label', d, { for : fieldMustExistCheckBox,  innerHTML : "Field must exist:" }, { float : "left", width : "130px" });
        UI.addDomElementChild('input', d, { id : fieldMustExistCheckBox,  type : "checkbox"}, { width : "20px", float : "left"});

        d = UI.addDomElementChild('div', fieldset, null, { class : fieldsetRowCSSClass,  "padding-top": "10px" });
        UI.addDomElementChild('label', d, { for : fieldNonEmptyCheckBox,  innerHTML : "Field is not empty:" }, { float : "left", width : "130px" });
        UI.addDomElementChild('input', d, { id : fieldNonEmptyCheckBox,  type : "checkbox"}, { width : "20px", float : "left"});

        UI.addDomElementChild('div', fieldset, null, { class : clearBothCSSClass } );
        UI.addDomElementChild('div', p, null, { class : clearBothCSSClass } );
        }
})();
