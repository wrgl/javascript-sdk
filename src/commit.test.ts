import { CommitsDict } from "./commit";
import { randCommitTree } from "./test-utils";

describe("CommitTree", () => {
  test("returns node at indices", () => {
    let tree = randCommitTree(0);
    expect(tree.getNode()).toEqual(tree.root);
    expect(tree.getNode(0)).toBeUndefined();
    expect(tree.getLeftMostNodeAtDepth(0)).toEqual(tree.root);
    expect(tree.getLeftMostNodeAtDepth(1)).toBeUndefined();

    tree = randCommitTree(2);
    expect(tree.getNode()).toEqual(tree.root);
    const commit1 = (tree.root.parentCommits as CommitsDict)[
      (tree.root.parents as string[])[0]
    ];
    expect(tree.getNode(0)).toEqual(commit1);
    const commit2 = (commit1.parentCommits as CommitsDict)[
      (commit1.parents as string[])[0]
    ];
    expect(tree.getNode(0, 0)).toEqual(commit2);
    expect(tree.getNode(1)).toBeUndefined();
    expect(tree.getNode(0, 1)).toBeUndefined();
    expect(tree.getNode(0, 0, 0)).toBeUndefined();
    expect(tree.getLeftMostNodeAtDepth(0)).toEqual(tree.root);
    expect(tree.getLeftMostNodeAtDepth(1)).toEqual(commit1);
    expect(tree.getLeftMostNodeAtDepth(2)).toEqual(commit2);
    expect(tree.getLeftMostNodeAtDepth(3)).toBeUndefined();
  });
});
