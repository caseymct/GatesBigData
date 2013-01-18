var nodeID;
var fontMaxSize = 24;
var fontMinSize = 10;

var ALT_QUERIES_KEY = "alternate_queries";
var NAME_KEY        = "name";
var COUNT_KEY       = "count";
var CHILDREN_KEY    = "children";

function size(count, level){
    return Math.min(fontMaxSize, Math.max(fontMinSize, (12 + count)/(Math.log(level + 1))));
}

function nodeText(json, name, count) {
    var altQueries = json[ALT_QUERIES_KEY];
    if (altQueries == undefined) {
        return name + " (" + count + ")";
    }

    var text = "";
    for(var i = 0; i < altQueries.length; i++) {
        text += altQueries[i][NAME_KEY] + " (" + altQueries[i][COUNT_KEY] + ")";
        if (i < altQueries.length - 1) {
            text += "\n";
        }
    }
    return text;
}

function returnIfDefined(t, defaultValue) {
    return (t == undefined) ? defaultValue : t;
}

function add(json, pid, tree, level) {
    nodeID++;
    var name  = returnIfDefined(json[NAME_KEY], "Undefined");
    var count = returnIfDefined(json[COUNT_KEY], 0);
    var text = nodeText(json, name, count);

    var node = tree.add(nodeID, pid, text, size(count, level));
    if (level > 1) node.isCollapsed = true;

    var children = json[CHILDREN_KEY];
    if (children != undefined) {
        var parentID = nodeID;
        for(var i = 0; i < children.length; i++) {
            add(children[i], parentID, tree, level + 1);
        }
    }
}


function redraw(paper, rtree, ltree) {
    rtree.UpdateTree();
    ltree.UpdateTree();
    paper.clear();
    rtree.UpdateTree();
    ltree.UpdateTree(rtree.rootNodeRaphaelElement);
}

function createTree(words, treeOptions) {
    nodeID = -1;
    var tree = new ECOTree(treeOptions);
    add(words, -1, tree, 0);
    return tree;
}

function makeWordTree(leftWords, rightWords, paper) {

    var treeOptions = { confOptions : { iRootOrientation : ECOTree.RO_LEFT, topXAdjustment : 100, topYAdjustment : 500 },
                           settings : { paper : paper} };

    var rtree = createTree(rightWords, treeOptions);

    treeOptions.confOptions.iRootOrientation = ECOTree.RO_RIGHT;
    var ltree = createTree(leftWords, treeOptions);

    redraw(paper, rtree, ltree);
}