package io.grpc.analytics;

import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.util.*;
import java.util.List;

public class TreePresentation {
    public static void main(String[] args) {
        // Hardcoded file path - change this to your actual file path
        String filePath = "/home/ismail/grpc-java-examples-master/node1/model_info.txt";
        
        // Visualize the tree from the specified file
        visualizeFromFile(filePath);
    }
    
    // Parse the tree string into a tree structure
    private static TreeNode parseTreeString(String treeString) {
        String[] lines = treeString.split("\n");
        
        // Create root node with generic info
        TreeNode root = new TreeNode("Decision Tree");
        
        // Stack to keep track of current node and its depth
        Stack<NodeDepthPair> stack = new Stack<>();
        stack.push(new NodeDepthPair(root, 0));
        
        boolean foundTreeStart = false;
        
        // Parse the tree structure
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i].trim();
            
            // Skip empty lines
            if (line.isEmpty()) {
                continue;
            }
            
            // Skip model info and tree weight lines
            if (line.startsWith("(RandomForestRegressionModel:") || 
                line.matches("\\(Tree \\d+ \\(weight \\d+\\.\\d+\\)\\)") ||
                line.startsWith("Tree") && line.contains("weight")) {
                continue;
            }
            
            // Determine the indentation level (depth) of the current line
            int depth = countLeadingSpaces(lines[i]) / 2; // Assuming 2 spaces per indentation level
            
            // Start tracking nodes once we find actual tree content
            if (!foundTreeStart && (line.startsWith("If") || line.startsWith("Predict"))) {
                foundTreeStart = true;
            }
            
            // Only process lines once we've found the tree start
            if (foundTreeStart) {
                // Pop stack until we find the parent for this node
                while (!stack.isEmpty() && stack.peek().depth >= depth) {
                    stack.pop();
                }
                
                if (!stack.isEmpty()) {
                    // Create a new node and add it to the parent
                    TreeNode newNode = new TreeNode(line);
                    stack.peek().node.addChild(newNode);
                    
                    // Push the new node to the stack
                    stack.push(new NodeDepthPair(newNode, depth));
                }
            }
        }
        
        return root;
    }
    
    // Count leading spaces to determine indentation level
    private static int countLeadingSpaces(String s) {
        int count = 0;
        while (count < s.length() && s.charAt(count) == ' ') {
            count++;
        }
        return count;
    }
    
    // Utility class to keep track of a node and its depth during parsing
    private static class NodeDepthPair {
        TreeNode node;
        int depth;
        
        NodeDepthPair(TreeNode node, int depth) {
            this.node = node;
            this.depth = depth;
        }
    }
    
    // Tree node class
    static class TreeNode {
        private String content;
        private List<TreeNode> children;
        
        public TreeNode(String content) {
            this.content = content;
            this.children = new ArrayList<>();
        }
        
        public void addChild(TreeNode child) {
            children.add(child);
        }
        
        public String getContent() {
            return content;
        }
        
        public List<TreeNode> getChildren() {
            return children;
        }
        
        public boolean isLeaf() {
            return children.isEmpty();
        }
        
        public boolean isDecisionNode() {
            return content.trim().startsWith("If");
        }
        
        public boolean isPredictNode() {
            return content.trim().startsWith("Predict");
        }
    }
    
    // Panel for visualizing the decision tree
    static class TreeVisualizationPanel extends JPanel {
        private static final int NODE_WIDTH = 220;
        private static final int NODE_HEIGHT = 50;
        private static final int VERTICAL_GAP = 80;
        private static final int HORIZONTAL_GAP = 40;
        
        private TreeNode root;
        private Map<TreeNode, Rectangle> nodePositions;
        private int totalWidth;
        private int totalHeight;
        
        public TreeVisualizationPanel(TreeNode root) {
            this.root = root;
            this.nodePositions = new HashMap<>();
            
            // Calculate the positions of all nodes
            calculatePositions();
            
            // Set the preferred size of the panel
            setPreferredSize(new Dimension(totalWidth + 100, totalHeight + 100));
            
            // Enable double buffering for smoother rendering
            setDoubleBuffered(true);
        }
        
        private void calculatePositions() {
            // First calculate the required width for each subtree
            Map<TreeNode, Integer> subtreeWidths = calculateSubtreeWidths(root);
            
            // Then calculate the position of each node
            totalWidth = positionNode(root, 50, 50, subtreeWidths);
            totalHeight = calculateHeight();
        }
        
        private Map<TreeNode, Integer> calculateSubtreeWidths(TreeNode node) {
            Map<TreeNode, Integer> widths = new HashMap<>();
            calculateSubtreeWidthsRecursive(node, widths);
            return widths;
        }
        
        private int calculateSubtreeWidthsRecursive(TreeNode node, Map<TreeNode, Integer> widths) {
            if (node.isLeaf()) {
                widths.put(node, NODE_WIDTH);
                return NODE_WIDTH;
            }
            
            int totalChildWidth = 0;
            for (TreeNode child : node.getChildren()) {
                totalChildWidth += calculateSubtreeWidthsRecursive(child, widths);
            }
            
            // Add horizontal gaps between children
            totalChildWidth += HORIZONTAL_GAP * (node.getChildren().size() - 1);
            
            // Ensure the subtree width is at least as wide as the node itself
            int nodeWidth = Math.max(NODE_WIDTH, totalChildWidth);
            widths.put(node, nodeWidth);
            
            return nodeWidth;
        }
        
        private int positionNode(TreeNode node, int x, int y, Map<TreeNode, Integer> subtreeWidths) {
            // Store the position of this node
            Rectangle nodeBounds = new Rectangle(x, y, NODE_WIDTH, NODE_HEIGHT);
            nodePositions.put(node, nodeBounds);
            
            if (node.isLeaf()) {
                return x + NODE_WIDTH;
            }
            
            // Position children
            int childX = x;
            for (TreeNode child : node.getChildren()) {
                // Calculate the width required by this child's subtree
                int childSubtreeWidth = subtreeWidths.get(child);
                
                int nextX = positionNode(child, childX, y + NODE_HEIGHT + VERTICAL_GAP, subtreeWidths);
                
                childX = nextX + HORIZONTAL_GAP;
            }
            
            // Adjust the x-position of the current node to center it over its children
            if (!node.getChildren().isEmpty()) {
                int firstChildX = nodePositions.get(node.getChildren().get(0)).x;
                int lastChildX = nodePositions.get(node.getChildren().get(node.getChildren().size() - 1)).x;
                int newX = firstChildX + (lastChildX - firstChildX) / 2 - NODE_WIDTH / 2;
                
                // Update the position of this node
                nodePositions.put(node, new Rectangle(newX, y, NODE_WIDTH, NODE_HEIGHT));
            }
            
            return childX;
        }
        
        private int calculateHeight() {
            int maxY = 0;
            for (Rectangle bounds : nodePositions.values()) {
                maxY = Math.max(maxY, bounds.y + bounds.height);
            }
            return maxY;
        }
        
        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);
            
            Graphics2D g2d = (Graphics2D) g;
            g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            
            // Draw connections between nodes
            drawConnections(g2d, root);
            
            // Draw nodes
            for (Map.Entry<TreeNode, Rectangle> entry : nodePositions.entrySet()) {
                TreeNode node = entry.getKey();
                Rectangle bounds = entry.getValue();
                
                // Determine node color based on type
                if (node.isPredictNode()) {
                    g2d.setColor(new Color(102, 187, 106)); // Green for prediction nodes
                } else if (node.isDecisionNode()) {
                    g2d.setColor(new Color(79, 195, 247)); // Blue for decision nodes
                } else {
                    g2d.setColor(new Color(255, 167, 38)); // Orange for other nodes (like tree header)
                }
                
                // Draw node background
                g2d.fillRoundRect(bounds.x, bounds.y, bounds.width, bounds.height, 10, 10);
                
                // Draw node border
                g2d.setColor(Color.BLACK);
                g2d.drawRoundRect(bounds.x, bounds.y, bounds.width, bounds.height, 10, 10);
                
                // Draw node content
                drawNodeContent(g2d, node, bounds);
            }
        }
        
        private void drawConnections(Graphics2D g2d, TreeNode node) {
            for (TreeNode child : node.getChildren()) {
                Rectangle parentBounds = nodePositions.get(node);
                Rectangle childBounds = nodePositions.get(child);
                
                // Determine the edge label
                String edgeLabel = "";
                if (node.isDecisionNode()) {
                    if (child.getContent().trim().startsWith("Else")) {
                        edgeLabel = "False";
                    } else {
                        edgeLabel = "True";
                    }
                }
                
                // Draw edge
                g2d.setColor(Color.BLACK);
                int x1 = parentBounds.x + parentBounds.width / 2;
                int y1 = parentBounds.y + parentBounds.height;
                int x2 = childBounds.x + childBounds.width / 2;
                int y2 = childBounds.y;
                
                g2d.drawLine(x1, y1, x2, y2);
                
                // Draw arrow at the end of the line
                int arrowSize = 8;
                int dx = x2 - x1;
                int dy = y2 - y1;
                double length = Math.sqrt(dx * dx + dy * dy);
                double ddx = dx / length;
                double ddy = dy / length;
                
                int[] xPoints = {x2, x2 - (int)(arrowSize * ddx + arrowSize * ddy), x2 - (int)(arrowSize * ddx - arrowSize * ddy)};
                int[] yPoints = {y2, y2 - (int)(arrowSize * ddy - arrowSize * ddx), y2 - (int)(arrowSize * ddy + arrowSize * ddx)};
                
                g2d.fillPolygon(xPoints, yPoints, 3);
                
                // Draw edge label if applicable
                if (!edgeLabel.isEmpty()) {
                    FontMetrics fm = g2d.getFontMetrics();
                    int labelWidth = fm.stringWidth(edgeLabel);
                    
                    // Position the label
                    int labelX = (x1 + x2) / 2 - labelWidth / 2;
                    int labelY = (y1 + y2) / 2 - 5;
                    
                    // Draw a small background for the label
                    g2d.setColor(new Color(255, 255, 255, 220));
                    g2d.fillRect(labelX - 2, labelY - fm.getAscent(), labelWidth + 4, fm.getHeight());
                    
                    // Draw the label
                    g2d.setColor(Color.BLACK);
                    g2d.drawString(edgeLabel, labelX, labelY);
                }
                
                // Recursively draw connections for children
                drawConnections(g2d, child);
            }
        }
        
        private void drawNodeContent(Graphics2D g2d, TreeNode node, Rectangle bounds) {
            g2d.setColor(Color.BLACK);
            
            // Split content into multiple lines if needed
            String content = node.getContent();
            
            // Use word wrapping
            List<String> lines = wrapText(content, g2d.getFontMetrics(), bounds.width - 10);
            
            // Position of the first line
            int y = bounds.y + 15;
            
            // Draw each line
            for (String line : lines) {
                int lineWidth = g2d.getFontMetrics().stringWidth(line);
                int x = bounds.x + (bounds.width - lineWidth) / 2; // Center the line
                
                g2d.drawString(line, x, y);
                y += g2d.getFontMetrics().getHeight();
            }
        }
        
        private List<String> wrapText(String text, FontMetrics fm, int maxWidth) {
            List<String> lines = new ArrayList<>();
            
            String[] words = text.split(" ");
            StringBuilder currentLine = new StringBuilder();
            
            for (String word : words) {
                // Check if adding this word would exceed maxWidth
                if (currentLine.length() > 0) {
                    int lineWidth = fm.stringWidth(currentLine + " " + word);
                    if (lineWidth <= maxWidth) {
                        currentLine.append(" ").append(word);
                    } else {
                        lines.add(currentLine.toString());
                        currentLine = new StringBuilder(word);
                    }
                } else {
                    currentLine.append(word);
                }
            }
            
            // Add the last line
            if (currentLine.length() > 0) {
                lines.add(currentLine.toString());
            }
            
            // If no line wrapping occurred, check if we need to truncate
            if (lines.size() == 1 && fm.stringWidth(lines.get(0)) > maxWidth) {
                String line = lines.get(0);
                StringBuilder truncatedLine = new StringBuilder();
                
                for (char c : line.toCharArray()) {
                    if (fm.stringWidth(truncatedLine.toString() + c + "...") <= maxWidth) {
                        truncatedLine.append(c);
                    } else {
                        break;
                    }
                }
                
                lines.set(0, truncatedLine + "...");
            }
            
            return lines;
        }
    }
    
    // Visualize tree from file
    public static void visualizeFromFile(String filePath) {
        try {
            StringBuilder content = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    content.append(line).append("\n");
                }
            }
            
            final String treeContent = content.toString();
            
            if (treeContent.trim().isEmpty()) {
                System.err.println("Error: The file is empty.");
                System.exit(1);
            }
            
            TreeNode root = parseTreeString(treeContent);
            
            SwingUtilities.invokeLater(() -> {
                JFrame frame = new JFrame("Decision Tree Visualization - " + new File(filePath).getName());
                frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                frame.setSize(1200, 800);
                
                TreeVisualizationPanel treePanel = new TreeVisualizationPanel(root);
                JScrollPane scrollPane = new JScrollPane(treePanel);
                frame.add(scrollPane);
                
                // Center the frame on the screen
                frame.setLocationRelativeTo(null);
                
                // Add a menu bar with export option
                JMenuBar menuBar = new JMenuBar();
                
                JMenu exportMenu = new JMenu("Export");
                JMenuItem saveAsImageItem = new JMenuItem("Save as PNG...");
                saveAsImageItem.addActionListener(e -> {
                    JFileChooser fc = new JFileChooser();
                    fc.setDialogTitle("Save as PNG");
                    fc.setFileFilter(new javax.swing.filechooser.FileFilter() {
                        @Override
                        public boolean accept(File f) {
                            return f.isDirectory() || f.getName().toLowerCase().endsWith(".png");
                        }
                        
                        @Override
                        public String getDescription() {
                            return "PNG Images (*.png)";
                        }
                    });
                    
                    if (fc.showSaveDialog(frame) == JFileChooser.APPROVE_OPTION) {
                        String savePath = fc.getSelectedFile().getAbsolutePath();
                        if (!savePath.toLowerCase().endsWith(".png")) {
                            savePath += ".png";
                        }
                        
                        // try {
                        //     // Create a buffered image from the panel
                        //     BufferedImage image = new BufferedImage(
                        //             treePanel.getPreferredSize().width,
                        //             treePanel.getPreferredSize().height,
                        //             BufferedImage.TYPE_INT_ARGB);
                        //     Graphics2D g2d = image.createGraphics();
                        //     treePanel.paint(g2d);
                        //     g2d.dispose();
                            
                        //     // Save to file
                        //     File outputFile = new File(savePath);
                        //     javax.imageio.ImageIO.write(image, "png", outputFile);
                            
                        //     System.out.println("Image saved successfully to: " + savePath);
                        // } catch (IOException ex) {
                        //     System.err.println("Error saving image: " + ex.getMessage());
                        // }
                    }
                });
                
                exportMenu.add(saveAsImageItem);
                menuBar.add(exportMenu);
                
                frame.setJMenuBar(menuBar);
                frame.setVisible(true);
            });
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}