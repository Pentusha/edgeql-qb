from edgeql_qb.operators import Node


class TestNode:
    def test_le_operator(self) -> None:
        n1 = Node(left='n1_left', op=':=', right=2)  # type: ignore[arg-type]
        n2 = Node(left='n2_left', op='+', right=4)  # type: ignore[arg-type]
        assert n1 < n2
        assert n1 < '*'
        assert n2 < '*'

    def test_gt_operator(self) -> None:
        n1 = Node(left='n1_left', op='+', right=2)  # type: ignore[arg-type]
        n2 = Node(left='n2_left', op='=', right=4)  # type: ignore[arg-type]
        assert n1 > n2
        assert n1 > ':='
        assert n2 > ':='

    def test_eq_operator(self) -> None:
        n1 = Node(left='n1_left', op='+', right=2)  # type: ignore[arg-type]
        n2 = '+'
        assert n1 == n2

    def test_ne_operator(self) -> None:
        n1 = Node(left='n1_left', op='+', right=2)  # type: ignore[arg-type]
        n2 = Node(left='n2_left', op='=', right=4)  # type: ignore[arg-type]
        assert n1 != n2

    def test_repr(self) -> None:
        n1 = Node(left='n1_left', op='+', right=2)  # type: ignore[arg-type]
        assert repr(n1) == "Node('n1_left','+',2)"
