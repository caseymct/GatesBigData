function makeTree(context, level, detail, orientation){
    var tree = {};
    var first, sentence, collapsed, subtree, key, max = 0, keys = [];

    for(var i = 0; i < context.length; i++){
        sentence = context[i];
        if(sentence.length > 0){
            first = sentence[0];
            if(!first){
                sentence = sentence.slice(1);
                first = sentence[0]
            }
            if(first){
                first = first.toLowerCase();
                if(tree[first]== undefined){
                    tree[first] = {key:first, after:[],level:level, count:1, tree:{}};
                }
                tree[first].after.push(sentence.slice(1));
                tree[first].count += 1;
            }
        }
    }
    for(key in tree){
        if(tree[key].count > max){
            max = tree[key].count;
        }
        keys.push(key);
    }

    var tmp;
    for(i = 0 ; i < keys.length; i++){
        key = keys[i];

        if(tree[key].count > (max*(100-detail)/100)){
            if(tree[key].after.length > 1){
                subtree = makeTree(tree[key].after, level+1, detail, orientation);
                collapsed = collapse(subtree, key, orientation);
                tmp = tree[key];
                delete tree[key];
                tree[collapsed.key] = {};
                tree[collapsed.key]["after"] = tmp["after"];
                tree[collapsed.key]["count"] = tmp["count"];
                tree[collapsed.key]["level"] = tmp["level"];
                tree[collapsed.key]["tree"] = collapsed.tree;
                tree[collapsed.key]["key"] = collapsed.key;
            }
        } else {
            delete tree[key];
        }
    }
    return sort(tree);
}

function compareSubTrees(t1, t2){
    return t2.count - t1.count;
}

function sort(tree){
    var tmp = [];
    for(key in tree){
        tmp.push(tree[key]);
    }
    tmp.sort(compareSubTrees);
    return tmp
}

function collapse(tree, key, orientation){
    if (tree.length == 1) {
        for (k in tree){
            if(orientation == WordTree.RO_LEFT){
                return collapse(tree[k].tree, key + " " + tree[k].key);
            }else if(orientation = WordTree.RO_RIGHT){
                return collapse(tree[k].tree, tree[k].key + " " + key);
            }
        }
    } else {
        return { tree : tree, "key" : key }
    }
}

function size(count, level){
    var size;
    if (count == "end"){
        size = 10;
    } else {
        size = Math.min(30, Math.max(12, (12 + count)/(Math.log(level + 1))));
    }
    return size;
}
/**
 @param context: the name of the root node
 **/
var nodeID;
function displayTree(context, tree, container, width, height, direction, paper, detail){
    var wordTree = new WordTree("tree", container, width, height, paper);
    wordTree.config.iRootOrientation = direction;
    //var queue = [];
    nodeID = 0;
    wordTree.add(nodeID/*root id*/, -1, context, 25, "black");
    nodeID += 1;
    addAll(tree, wordTree, 0/*parent*/, 3);
    wordTree.UpdateTree();
    return wordTree;
}

function addAll(tree, wordTree, parent, stoplevel){
    var key;
    var dsc;

    for(var i = 0; i < tree.length; i++){
        key = tree[i].key + " (" + tree[i].count + ")";
        wordTree.add(nodeID, parent, key, size(tree[i].count, tree[i].level));
        nodeID += 1;

        //if (tree[i].level < stoplevel) {
            if (tree[i].after.length > 1){
                addAll(tree[i].tree, wordTree, nodeID-1, "black");
            } else {
                dsc = "";
                if(wordTree.config.iRootOrientation == WordTree.RO_LEFT){
                    dsc = tree[i].after[0].join(" ")
                }else{
                    dsc = tree[i].after[0].reverse().join(" ");
                }
                wordTree.add(nodeID, nodeID-1, dsc, size("end", ""), "#FF3300");//"#999");
                nodeID +=1;
            }
        //}
    }
}


function makeWordTree(sentences, context, detail, container, width, height, direction, paper){
    debugger;
    var tree = makeTree(sentences, 1, detail, direction);
    return displayTree(context, tree, container, width, height, direction, paper, detail);
}