"""
Visualization functionality for dependency graphs.
"""

from typing import Dict, Any
from pathlib import Path

from ..shared.types import DependencyGraph
from ..shared.exceptions import OutputGenerationError


class DependencyVisualizer:
    """Handles visualization of dependency graphs."""
    
    def print_tree(self, graph: DependencyGraph) -> None:
        """
        Print a visual tree representation of the dependency graph.
        
        Args:
            graph: The dependency graph
        """
        if not graph['execution_order']:
            print("No valid execution order (circular dependencies detected)")
            return
        
        print("\nDependency Tree Visualization:")
        print("=" * 50)
        
        # Build a tree structure from execution order
        tree = self._build_tree_structure(graph)
        
        # Print the tree
        self._print_tree_recursive(tree, "", set())
    
    def print_summary(self, graph: DependencyGraph) -> None:
        """
        Print a comprehensive summary of the dependency graph.
        
        Args:
            graph: The dependency graph
        """
        print("\n" + "=" * 60)
        print("DEPENDENCY GRAPH SUMMARY")
        print("=" * 60)
        
        print("\n📊 Statistics:")
        print(f"  • Total tables: {len(graph['nodes'])}")
        print(f"  • Total dependencies: {len(graph['edges'])}")
        print(f"  • Circular dependencies: {len(graph['cycles'])}")
        
        if graph['execution_order']:
            print("\n🔄 Execution Order:")
            for i, table in enumerate(graph['execution_order'], 1):
                print(f"  {i}. {table}")
        
        print("\n📋 Table Details:")
        for table in sorted(graph['nodes']):
            deps = graph['dependencies'][table]
            dependents = graph['dependents'][table]
            
            print(f"\n  📄 {table}")
            if deps:
                print(f"     Depends on: {', '.join(deps)}")
            else:
                print("     No dependencies (base table)")
            
            if dependents:
                print(f"     Used by: {', '.join(dependents)}")
            else:
                print("     No dependents (leaf table)")
        
        if graph['cycles']:
            print("\n⚠️  Circular Dependencies:")
            for i, cycle in enumerate(graph['cycles'], 1):
                print(f"  {i}. {' → '.join(cycle)} → {cycle[0]}")
        
        print("\n" + "=" * 60)
    
    def generate_mermaid_diagram(self, graph: DependencyGraph) -> str:
        """
        Generate a Mermaid diagram representation of the dependency graph.
        
        Args:
            graph: The dependency graph
            
        Returns:
            Mermaid diagram as a string
        """
        mermaid_lines = ["graph TD"]
        
        # Add nodes
        for node in sorted(graph['nodes']):
            # Escape special characters in node names for Mermaid
            safe_node = self._escape_mermaid_node(node)
            mermaid_lines.append(f"    {safe_node}[\"{node}\"]")
        
        # Add edges (dependencies)
        for dep, table in graph['edges']:
            safe_dep = self._escape_mermaid_node(dep)
            safe_table = self._escape_mermaid_node(table)
            mermaid_lines.append(f"    {safe_dep} --> {safe_table}")
        
        # Add execution order information as comments
        if graph['execution_order']:
            mermaid_lines.append("")
            mermaid_lines.append("    %% Execution Order:")
            for i, table in enumerate(graph['execution_order'], 1):
                safe_table = self._escape_mermaid_node(table)
                mermaid_lines.append(f"    %% {i}. {table}")
        
        # Add cycle information if present
        if graph['cycles']:
            mermaid_lines.append("")
            mermaid_lines.append("    %% Circular Dependencies Detected:")
            for cycle in graph['cycles']:
                cycle_str = " → ".join(cycle) + f" → {cycle[0]}"
                mermaid_lines.append(f"    %% {cycle_str}")
        
        return "\n".join(mermaid_lines)
    
    def save_mermaid_diagram(self, graph: DependencyGraph, output_file: str = "output/dependency_graph.mmd") -> None:
        """
        Save the dependency graph as a Mermaid diagram file.
        
        Args:
            graph: The dependency graph
            output_file: Output file path
        """
        try:
            # Ensure output directory exists
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            mermaid_content = self.generate_mermaid_diagram(graph)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(mermaid_content)
            
            print(f"Mermaid diagram saved to {output_file}")
            print(f"Total nodes: {len(graph['nodes'])}")
            print(f"Total dependencies: {len(graph['edges'])}")
            if graph['cycles']:
                print(f"⚠️  Warning: {len(graph['cycles'])} circular dependencies detected!")
        except Exception as e:
            raise OutputGenerationError(f"Failed to save Mermaid diagram: {e}")
    
    def _escape_mermaid_node(self, node_name: str) -> str:
        """
        Escape special characters in node names for Mermaid compatibility.
        
        Args:
            node_name: The node name to escape
            
        Returns:
            Escaped node name safe for Mermaid
        """
        # Replace special characters that might cause issues in Mermaid
        escaped = node_name.replace(".", "_")
        escaped = escaped.replace("-", "_")
        escaped = escaped.replace(" ", "_")
        escaped = escaped.replace("(", "_")
        escaped = escaped.replace(")", "_")
        escaped = escaped.replace("[", "_")
        escaped = escaped.replace("]", "_")
        escaped = escaped.replace("{", "_")
        escaped = escaped.replace("}", "_")
        escaped = escaped.replace(":", "_")
        escaped = escaped.replace(";", "_")
        escaped = escaped.replace(",", "_")
        escaped = escaped.replace("'", "_")
        escaped = escaped.replace('"', "_")
        
        # Ensure it starts with a letter or underscore
        if escaped and not escaped[0].isalpha() and escaped[0] != "_":
            escaped = "_" + escaped
        
        return escaped
    
    def _build_tree_structure(self, graph: DependencyGraph) -> Dict[str, Any]:
        """
        Build a tree structure from the dependency graph.
        
        Args:
            graph: The dependency graph
            
        Returns:
            Tree structure with root nodes and their children
        """
        dependencies = graph['dependencies']
        dependents = graph['dependents']
        
        # Find root nodes (nodes with no dependencies)
        root_nodes = [node for node, deps in dependencies.items() if not deps]
        
        # Build tree recursively
        tree = {}
        for root in root_nodes:
            tree[root] = self._build_subtree(root, dependencies, dependents)
        
        return tree
    
    def _build_subtree(self, node: str, dependencies: Dict[str, list], 
                      dependents: Dict[str, list]) -> Dict[str, Any]:
        """
        Build a subtree starting from a given node.
        
        Args:
            node: The current node
            dependencies: Dependencies mapping
            dependents: Dependents mapping
            
        Returns:
            Subtree structure
        """
        children = {}
        for child in dependents.get(node, []):
            children[child] = self._build_subtree(child, dependencies, dependents)
        return children
    
    def _print_tree_recursive(self, tree: Dict[str, Any], prefix: str, 
                             visited: set) -> None:
        """
        Recursively print the tree structure.
        
        Args:
            tree: Tree structure to print
            prefix: Current prefix for indentation
            is_last: Whether this is the last item at this level
            visited: Set of already visited nodes (to handle cycles)
        """
        items = list(tree.items())
        
        for i, (node, children) in enumerate(items):
            is_last_item = i == len(items) - 1
            
            # Choose the appropriate tree characters
            if is_last_item:
                connector = "└── "
                next_prefix = prefix + "    "
            else:
                connector = "├── "
                next_prefix = prefix + "│   "
            
            # Handle cycles
            if node in visited:
                print(f"{prefix}{connector}{node} (circular reference)")
                continue
            
            print(f"{prefix}{connector}{node}")
            
            # Add to visited set and recurse
            visited.add(node)
            if children:
                self._print_tree_recursive(children, next_prefix, visited.copy())
            visited.remove(node)
